#import pertinent libraries
import re
import csv
import time
import requests
import feedparser
import pandas as pd
from bs4 import BeautifulSoup
from prettytable import PrettyTable
from datetime import datetime, timedelta



# Function to get rss feed and convert it to a dataframe
def rssFeed_events_to_dataframe(url):
    # Fetch the RSS feed
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        rss_feed_content = response.content
        
        # Parse the RSS feed using feedparser
        feed = feedparser.parse(rss_feed_content)
        
        # List to store rows of data
        data = []
    
        # Iterate through the feed entries and extract relevant data
        for entry in feed.entries:
            title = entry.get('title', 'No Title')
            description = entry.get('summary', 'No Description')
            
            # Parse the description HTML to extract table data
            soup = BeautifulSoup(description, 'html.parser')
            rows = soup.find_all('tr')
    
            if rows:
                data_cells = rows[1].find_all('td')  # Skipping header row (rows[0])
                if len(data_cells) == 5:  # Ensure there are 5 columns (Time left, Impact, Previous, Consensus, Actual)
                    time_left = data_cells[0].get_text(strip=True)
                    impact = data_cells[1].get_text(strip=True)
                    previous = data_cells[2].get_text(strip=True)
                    consensus = data_cells[3].get_text(strip=True)
                    actual = data_cells[4].get_text(strip=True)
                    
                    # Append the extracted data to the list as a dictionary
                    data.append({
                        "Title": title,
                        "Time Left": time_left,
                        "Impact": impact,
                        "Previous": previous,
                        "Consensus": consensus,
                        "Actual": actual
                    })
            else:
                # If no table found, add title only with "N/A" for other fields
                data.append({
                    "Title": title,
                    "Time Left": "N/A",
                    "Impact": "N/A",
                    "Previous": "N/A",
                    "Consensus": "N/A",
                    "Actual": "N/A"
                })
        
        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(data)
        
        # Return the DataFrame
        return df
    
    else:
        print(f"Failed to retrieve RSS feed. Status code: {response.status_code}")
        return None
    


# Function to extract specific/multiple data for an event(s) saved in a dataframe
def extract_listed_events_data_DF(df, event_index_list, data_type_list):
    dict = {}
    
    # Loop through each event
    for event_index in event_index_list:
        # Check index bounds
        if event_index >= len(df):
            return f"Index {event_index} is out of bounds."

        # Get the event title and deduce whether the statistic would be inverse
        event_row = df.iloc[event_index]
        event_title = event_row["Title"]
        invert_statistic = "unemployment" in event_title.lower()

        # Initialize sub-dict if not already
        if event_title not in dict:
            dict[event_title] = {}
        
        # Loop through the required data types
        for data_type in data_type_list:
           
            # Check if the requested data type is valid
            if data_type not in df.columns:
                return f"Invalid data type requested: {data_type}. Choose from 'Previous', 'Consensus', or 'Actual'."
            
            # Extract the specific data (Previous, Consensus, Actual)
            result = event_row[data_type]  # Getting the first match
            
            # Strip the returned value of any non-numerical
            dict[event_title][data_type] = strip_symbols(result)
        dict[event_title]["Invert_statistic"] = invert_statistic

    return dict



def strip_symbols(value):
    return re.sub(r"[^\d\.-]", "", str(value))



# Function to calculate sleep time until release buffer seconds before the event time
def wait_until_event(release_time_str, release_buffer):
    #release_time = datetime.strptime(release_time_str, "%H:%M")
    current_time = datetime.now()
    
    # Calculate the time difference minus {release buffer} seconds
    time_diff = release_time_str - current_time - timedelta(seconds=release_buffer)
    
    # Check if the release time has already passed
    if time_diff.total_seconds() <= 0:
        return 0  # No sleep, start checking immediately
    
    # Sleep until {release_buffer} seconds before the event
    print(f"Sleeping for {time_diff.total_seconds()} seconds until {release_buffer} seconds before the event...")
    return time_diff.total_seconds()
  


def fetch_rss_feed(session, url):
    try:
        response = session.get(url)
        response.raise_for_status()  # Ensure we got a successful response
        feed_content = response.content
        feed = feedparser.parse(feed_content)
        
        # Extract relevant information from the feed
        data = []
        for entry in feed.entries:
            title = entry.title.lower()
            description = entry.description
            data.append({"title": title, "description": description})
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        return df
    
    except requests.RequestException as e:
        print(f"Network error: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error



def extract_actual_value(description):
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(description, "html.parser")
    
    # Find the table row that contains the actual data
    table_rows = soup.find_all("tr")
    
    # Check if there are enough rows (ensure there is at least one data row)
    if len(table_rows) > 1:
        # Get the second row, which contains the event data
        data_row = table_rows[1]
        
        # Find all cells in that row
        cells = data_row.find_all("td")
        
        # Ensure that there are enough cells to extract the 'Actual' value
        if len(cells) >= 5:
            actual_value = cells[4].text.strip()  # Extract the 'Actual' value from the 5th cell (index 4)
            return strip_symbols(actual_value) if actual_value else None  # Return the actual value or None if it's empty
    
    # Return None if the 'Actual' value could not be found
    return None
    


def check_events_in_dataframe(df, event_dict):
    events_data = []
    num_events = len(event_dict)  # Count the number of events

    for event_title, event_data in event_dict.items():
        previous = event_data["Previous"]
        consensus = event_data["Consensus"]
        invert_stat = event_data["Invert_statistic"]

        # Extract the event description for this event from the DataFrame
        df_filtered = df[df['title'].str.contains(event_title, case=False, na=False)]
        
        if not df_filtered.empty:
            description = df_filtered.iloc[0]['description']  # Description of the first match
            actual = extract_actual_value(description)
            
            # Append event details, including the number of events
            events_data.append([event_title, previous, consensus, actual, invert_stat, num_events])
        else:
            # Append None for actual if no match found
            events_data.append([event_title, previous, consensus, None, invert_stat, num_events])

    return events_data

   

def monitor_listed_events_for_update_and_send(url, event_dict, release_time_str, release_buffer, file_path):
    # Create a session object
    session = requests.Session()
    
    try:
        # Pre-calculate the release time
        current_date = datetime.now().strftime("%Y-%m-%d")

        # Append time and seconds to the date
        release_time_str = f"{current_date} {release_time_str}"
        release_time = datetime.strptime(release_time_str, "%Y-%m-%d %H:%M")
        
        # Wait until it's {release_buffer} seconds before the event time
        sleep_time = wait_until_event(release_time, release_buffer)
        if sleep_time > 0:
            time.sleep(sleep_time)

        print("Awake!")

        # Pre-allocate the list with the number of events
        num_events = len(event_dict)
        all_events_data = [[] for _ in range(num_events)]

        # Initialize polling interval settings
        polling_interval = 2
        final_polling_interval = 0.5

        while True:
            # Fetch and process the RSS feed
            df = fetch_rss_feed(session, url)
            
            if df.empty:
                print("No data fetched from RSS feed. Retrying...")
                time.sleep(polling_interval)  # Wait before retrying
                continue
            
            # Check for updates using the dataframe
            events_data = check_events_in_dataframe(df, event_dict)

            # Check if all events have their 'Actual' values
            all_available = all([data[3] is not None for data in events_data])
            if all_available:
                all_events_data = events_data
                break
            else:
                print("Not all 'Actual' values are available yet. Retrying...")
                # Adjust polling interval based on proximity to the release time
                if datetime.now() >= release_time:
                    polling_interval = final_polling_interval
                time.sleep(polling_interval)  # Wait before retrying

        # Write all events to file
        with open(file_path, "w", newline="") as file:
            csv_writer = csv.writer(file)
            #csv_writer.writerow(["Event", "Previous", "Consensus", "Actual", "Invert_statistic", "Num_events"])
            csv_writer.writerows(all_events_data)
        
        actual_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"Data updated and written to file successfully at {actual_time}")
    
    finally:
        # Close the session
        session.close()

# This is a comment to test my continuous delivery pipeline