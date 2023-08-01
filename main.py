import os
import csv
import requests
import threading

BASE_URL = "https://instagram-scraper-2022.p.rapidapi.com/ig"
headers = {
    "X-RapidAPI-Key": "eae1b73cb9mshc5dece8c05b7c84p172949jsned39caa145b8",
    "X-RapidAPI-Host": "instagram-scraper-2022.p.rapidapi.com"
}

followers_list = []
followers_lock = threading.Lock()

def append_row_to_csv(file_path, row_data):
    print(f"Appending {row_data} to {file_path}")
    file_exists = os.path.isfile(file_path)
    mode = "a" if file_exists else "w"
    if mode == "w":
        header = [
            "Username",
            "FullName",
            "Followers Count",
            "Public Email",
            "Country Code",
            "Public Phone",
            "Whatsapp Number",
            "Whatsapp URL",  # Add a new column for the URL
        ]
        print(f"Writing header {header} to {file_path}")
        with followers_lock:
            with open(file_path, mode, newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(header)

    # Generate the URL based on the data in row_data
    whatsapp_url = f"https://wa.me/{row_data[5]}{row_data[6]}"  # Assuming row_data follows the order [Username, FullName, Followers Count, Public Email, Country Code, Public Phone, Whatsapp Number]

    # Append the URL to row_data
    row_data.append(whatsapp_url)

    with followers_lock:
        print(f"Appending {row_data} to {file_path}")
        with open(file_path, "a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(row_data)

def getUserId(username):
    param = {
        "user": username
    }
    res = requests.get(f"{BASE_URL}/info_username/", headers=headers, params=param)

    data = res.json()
    if data.get('status', None) == "ok":
        return data.get('user').get('pk')

def getUser(username):
    param = {
        "user": username
    }
    res = requests.get(f"{BASE_URL}/info_username/", headers=headers, params=param)

    data = res.json()
    if data.get('status', None) == "ok":
        return data.get('user')

def getFollowers(userId, max_id=None):
    param = {
        'id_user': userId
    }
    if max_id is not None:
        param['next_max_id'] = max_id
    request = requests.get(f"{BASE_URL}/followers/", headers=headers, params=param)
    res = request.json()
    if res.get('status', None) == "ok" and res.get('next_max_id', None) is not None:
        users = res.get('users', [])
        public_users = [item for item in users if not item["is_private"]]
        followers_list.extend(public_users)
        print("fetched and again call")
        getFollowers(userId=userId, max_id=res.get('next_max_id'))
    elif res.get('status', None) == "ok":
        users = res.get('users', [])
        public_users = [item for item in users if not item["is_private"]]
        followers_list.extend(public_users)
        print("finished")

def fetch_and_save_user_info(usernames, file_path):
    for username in usernames:
        print(f"Processing {username.get('username')} - Thread: {threading.current_thread().name}")
        user = getUser(username.get('username'))
        if user and user.get('public_email') and user['public_email'] != "":
            temp = [
                user.get('username'),
                user.get('full_name'),
                user.get('follower_count'),
                user.get('public_email'),
                user.get('country_code'),
                user.get('public_phone_country_code'),
                user.get('public_phone_number'),
            ]
            append_row_to_csv(file_path=file_path, row_data=temp)

def main():
    page = input("Enter page name: ")
    file_path = f"output/{page}.csv"
    user = getUserId(page)
    print(user)
    getFollowers(userId=user)
    print(len(followers_list), "followers")

    # Split the followers_list into smaller chunks to process in parallel
    num_threads = 300  # You can adjust this number based on your system's capabilities
    chunks = [followers_list[i:i + len(followers_list) // num_threads] for i in range(0, len(followers_list), len(followers_list) // num_threads)]

    # Create and start the threads
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=fetch_and_save_user_info, args=(chunks[i], file_path))  # Pass file_path here
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete with a timeout of 60 seconds
    for thread in threads:
        thread.join(60)  # Adjust the timeout value as needed

if __name__ == "__main__":
    main()
