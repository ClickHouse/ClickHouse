#!/usr/bin/python


# create_vm_runner.py
#   - Create a Fyre VM using curl command.
#   - SSH in root user of VM, installed dependencies and added user
#   - SSH in new_user of vm.
#   - Getting the latest release version of runner and getting the access_token for the installation of runner
#   - Installation of self hosted runner on VM
#

# Parameters :
#   username_root,        # The username for the root user on the virtual machine.
#   repo_owner,           # The owner or organization name of the GitHub repository.
#   repo_name,            # The name of the GitHub repository.
#   token,                # The access token or authentication token used for GitHub API operations.
#   ssh_public_key_path,  # The file path of the SSH public key used for authentication.
#   fyre_username,        # The username for accessing the Fyre API.
#   fyre_apikey,          # The API key for accessing the Fyre API.
#   cpu,                  # The number of CPUs assigned to the virtual machine.
#   memory,               # The amount of memory (in GB) assigned to the virtual machine.
#   os_flavor,            # The flavor or version of the operating system for the virtual machine.
#   labels,               # Labels we want to pass to be associated with the self-hosted runner for GitHub actions.
#   platform,             # Platform for the VM

import json
import paramiko
import requests
import time
import uuid
import sys
import subprocess

# ssh_public_key_path = "/Users/umangbrahmbhatt/.ssh/id_rsa.pub"
# fyre_username = "umang.brahmbhatt"
# fyre_apikey = "********"
# cpu =  "2"
# memory = "16"
# os_flavor =  "ubuntu 22.04"
# labels = "first"

# Create VM
def create_runner_vm(username, fyre_apikey, custom_uuid, ssh_public_key_path, cpu, memory, os_flavor, platform):

    # Read the SSH public key file
    with open(ssh_public_key_path, 'r') as f:
        ssh_public_key = f.read().strip()
        print(ssh_public_key)

    # Prepare the request payload
    payload = {
        "cluster_prefix": f"Runner-{custom_uuid}",
        "clusterconfig": {
            "instance_type": "virtual_server",
            "platform": platform
        },
        f"Runner-{custom_uuid}": [
            {
                "name": "GitHub",
                "cpu": cpu,
                "memory": memory,
                "os": os_flavor,
                "publicvlan": "y",
                "count": 1
            }
        ],
        "fyre": {
            "creds": {
                "username": username,
                "api_key": fyre_apikey,
                "public_key": ssh_public_key
            }
        }
    }

    # Convert the payload to JSON
    json_payload = json.dumps(payload)

    # API URL
    url = "https://api.fyre.ibm.com/rest/v1/?operation=build"

    # Set the headers
    headers = {
        "Content-Type": "application/json"
    }

    # Send the POST request to build vm
    response = requests.post(url, data=json_payload, headers=headers, auth=(username, fyre_apikey), verify=False)

    # Check the request_id in response
    if response.status_code == 200:
        response_data = response.json()
        request_id = response_data.get('request_id')
        
        # This will check that the request for creating VM is successful
        if request_id:
            print("Request ID:", request_id)

            # Construct the new URL with the updated request_id
            url = f"https://api.fyre.ibm.com/rest/v1/?operation=query&request=showrequests&request_id={request_id}"

            # Make the GET request
            curl_command = f'curl -X GET -k -u "{username}:{fyre_apikey}" "{url}"'

            # Execute the curl command and capture the output
            try:
                result = subprocess.run(curl_command, shell=True, check=True, capture_output=True, text=True)
                response_text = result.stdout
                print(response_text)

                # Check the response
                if result.returncode == 0:
                    response_data = json.loads(response_text)
                    print("Response data:", response_data)  # Debugging statement
                    request_info = response_data.get('request')
                    if request_info:
                        request = request_info[0]
                        error_details = request.get('error_details')
                        print("Error details:", error_details)

                        if error_details == "0":
                            # Continue with creating the VM
                            print("No errors, creating VM")
                        else:
                            # Print the error message
                            print(f"Error during creating VM: {error_details}")
                            sys.exit(1)  # Exit the program with an error status
                    else:
                        print("No request information found in the response.")
                        sys.exit(1)  # Exit the program with an error status                            
                else:
                    print("Error occurred during the curl command execution.")
                    print("Command output:", response_text)
                    sys.exit(1)  # Exit the program with an error status

            except subprocess.CalledProcessError as e:
                print("An error occurred:", e)
        else:
            print("Request ID not found in the response.")
            sys.exit(1)
    else:
        print("Error:", response.text)
        sys.exit(1) 

    if response.status_code == 200:
        print("Request successful!")
        print("Response:")
        print(response.text)
    else:
        print("Error:", response.text)

    hostname = f"Runner-{custom_uuid}-GitHub.fyre.ibm.com"

    return hostname, ssh_public_key

# SSH into VM and add user
def ssh_vm_root_add_user(username_root, hostname, ssh_public_key):

    # Sleep for 3 minutes
    time.sleep(180)

    print(ssh_public_key)

    # SSH client setup
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Debug
        # paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)

        # Connect to the VM using SSH private key authentication
        ssh.connect(hostname=hostname, username=username_root)
        print("SSH connection established successfully.")            

        # Execute the "ls" command
        stdin, stdout, stderr = ssh.exec_command("ls")

        # Read the output of the command
        output = stdout.read().decode()

        # Print the contents of the home directory
        print("Home directory contents:")
        print(output)

        # new user details
        new_user = "runner"

        # Append the line to allow the user to run sudo without a password
        sudoers_line = f"{new_user}   ALL=(ALL:ALL) NOPASSWD:ALL"
        sudoers_append_command = f'echo "{sudoers_line}" | sudo tee -a /etc/sudoers'
        stdin, stdout, stderr = ssh.exec_command(sudoers_append_command)
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            print(f'Failed to add sudoers line for {new_user}')
            print(stderr.read().decode())
        else:
            print(f'Sudoers line added successfully for {new_user}')

        # Installation dependcies and create user
        commands = [
            f'sudo adduser --disabled-password --gecos "" {new_user} && \
            sudo usermod -aG sudo {new_user} && \
            sudo su - {new_user} -c "mkdir -p ~/.ssh && echo \'{ssh_public_key}\' >> ~/.ssh/authorized_keys"'
        ]

        # Execute the commands
        for command in commands:
            stdin, stdout, stderr = ssh.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                print(f'Command execution failed: {command}')
                print(stderr.read().decode())
            else:
                print(f'Command executed successfully: {command}')

    except Exception as e:
        print("An error occurred:", str(e))
        
    finally:
        # Close the SSH connection
        ssh.close()

        return new_user

# SSH into new_user and install dependencies
def ssh_vm_user_install_dependencies(new_user, hostname, repo_owner, repo_name, token, custom_uuid, labels):

    # Sleep for 3 minutes
    time.sleep(180)

    # SSH client setup
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to fyre vm with for new_user
        ssh.connect(hostname=hostname, username=new_user)         

        print("Successful login as new_user")

        # Installation dependcies and create user
        commands = [
            'sudo apt update',
            'sudo apt install -y openssh-server',
            'sudo apt install -y git',
            'sudo apt install -y apt-transport-https \
                                at \
                                software-properties-common \
                                atop \
                                binfmt-support \
                                build-essential \
                                ca-certificates \
                                curl \
                                gnupg \
                                jq \
                                lsb-release \
                                pigz \
                                ripgrep \
                                zstd \
                                python3-dev \
                                python3-pip \
                                qemu-user-static \
                                unzip',
            # Install docker
            'curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg',
            'echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null',
            'sudo apt update',
            'sudo apt install -y docker-ce docker-ce-cli containerd.io',
            'sudo usermod -aG docker runner',
            # install python
            'sudo apt install -y python3',
            'sudo pip install pygithub requests urllib3 unidiff dohq-artifactory boto3',
        ]

        # Execute the commands
        for command in commands:
            stdin, stdout, stderr = ssh.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            if exit_code != 0:
                print(f'Command execution failed: {command}')
                print(stderr.read().decode())
            else:
                print(f'Command executed successfully: {command}')                 

        # Execute commands or perform operations as the new user
        latest_version = get_latest_release_version()
        print("Latest version:", latest_version)
        create_self_hosted_runner(ssh, repo_owner, repo_name, token, latest_version, custom_uuid, labels)

    finally:
        # Close the SSH connection
        ssh.close()

# Get latest release version of runner  
def get_latest_release_version():
    api_url = "https://api.github.com/repos/actions/runner/releases/latest"
    response = requests.get(api_url)
    if response.status_code == 200:
        tag_name = response.json().get("tag_name")
        print(tag_name)  # Move the print statement here if you want to print the value
        return tag_name
    else:
        raise Exception("Failed to retrieve the latest release version")

# Get access_token to install runner
def get_access_token(repo_owner, repo_name, token):
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/runners/registration-token"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}"
    }
    response = requests.post(api_url, headers=headers)
    if response.status_code == 201:
        return response.json().get("token")
    else:
        raise Exception("Failed to retrieve access token")

# Installation of self hosted runner
def create_self_hosted_runner(ssh, repo_owner, repo_name, token, latest_version, custom_uuid, labels):
    # Get the access token from the GitHub API
    access_token = get_access_token(repo_owner, repo_name, token)
    print(access_token)
    print("latest-version:", latest_version)
    custom_latest_version= latest_version.lstrip("v")
    print(custom_latest_version)
    print(custom_uuid)

    # Create a folder for the actions runner on the remote VM, cd into it, apply commands
    command = f'''
        mkdir actions-runner;
        sleep 5;
        cd actions-runner;
        sleep 5;
        # Download the latest runner package on the remote VM
        curl -o actions-runner-linux-x64-{custom_latest_version}.tar.gz -L "https://github.com/actions/runner/releases/download/{latest_version}/actions-runner-linux-x64-{custom_latest_version}.tar.gz";
        sleep 5;
        # Optional: Validate the hash on the remote VM
        echo "292e8770bdeafca135c2c06cd5426f9dda49a775568f45fcc25cc2b576afc12f  actions-runner-linux-x64-{custom_latest_version}.tar.gz" | shasum -a 256 -c;
        sleep 5;
        # Extract the installer on the remote VM
        tar xzf actions-runner-linux-x64-{custom_latest_version}.tar.gz;
        sleep 5;
        echo "config stage will start";
        ./config.sh --url "https://github.com/{repo_owner}/{repo_name}" --token {access_token} --runnergroup "Default" --name "runner-{custom_uuid}" --labels "{labels}" --work "_work";       
        sleep 30;
        echo "Runner creation complete"
        '''

    stdin, stdout, stderr = ssh.exec_command(command)
    output = stdout.read().decode()  # Read the output of the command
    print(output)    
    time.sleep(15)

if __name__ == "__main__":
    # Check if all the required arguments are provided
    if len(sys.argv) < 9:
        print("Insufficient arguments. Usage: python script.py fyre_username fyre_apikey cpu memory os_version labels ssh_public_key_path")
    else:
        fyre_username = sys.argv[1]
        fyre_apikey = sys.argv[2]
        cpu = sys.argv[3]
        memory = sys.argv[4]
        os_flavor = sys.argv[5]
        labels = sys.argv[6]
        ssh_public_key_path = sys.argv[7]
        platform = sys.argv[8]

        # Parameters
        username_root = "root"
        repo_owner = "ClibMouse"
        repo_name = "ClickHouse"
        token = "*****"        

        # Generate a UUID
        uuid_value = uuid.uuid4()
        # Truncate the UUID to 8 digits
        custom_uuid = str(uuid_value)[:8]
        print(custom_uuid)

        # Create runner VM
        hostname, ssh_public_key = create_runner_vm(fyre_username, fyre_apikey, custom_uuid, ssh_public_key_path, cpu, memory, os_flavor, platform)
        # ssh and add a new user
        new_user = ssh_vm_root_add_user(username_root, hostname, ssh_public_key)
        # create_self_hosted_runner
        ssh_vm_user_install_dependencies(new_user, hostname, repo_owner, repo_name, token, custom_uuid, labels)


# how to call script
# python3 script.py {fyre_username} {fyre_apikey} {cpu} {memory} {os_flavor} {labels} {ssh_public_key_path} {platform}
