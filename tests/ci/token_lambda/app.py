#!/usr/bin/env python3

import requests
import argparse
import jwt
import sys
import json
import time

def get_installation_id(jwt_token):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get("https://api.github.com/app/installations", headers=headers)
    response.raise_for_status()
    data = response.json()
    return data[0]['id']

def get_access_token(jwt_token, installation_id):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.post(f"https://api.github.com/app/installations/{installation_id}/access_tokens", headers=headers)
    response.raise_for_status()
    data = response.json()
    return data['token']

def get_runner_registration_token(access_token):
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.post("https://api.github.com/orgs/ClickHouse/actions/runners/registration-token", headers=headers)
    response.raise_for_status()
    data = response.json()
    return data['token']

def get_key_and_app_from_aws():
    import boto3
    secret_name = "clickhouse_github_secret_key"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    data = json.loads(get_secret_value_response['SecretString'])
    return data['clickhouse-app-key'], int(data['clickhouse-app-id'])


def main(github_secret_key, github_app_id, push_to_ssm, ssm_parameter_name):
    payload = {
        "iat": int(time.time()) - 60,
        "exp": int(time.time()) + (10 * 60),
        "iss": github_app_id,
    }

    encoded_jwt = jwt.encode(payload, github_secret_key, algorithm="RS256")
    installation_id = get_installation_id(encoded_jwt)
    access_token = get_access_token(encoded_jwt, installation_id)
    runner_registration_token = get_runner_registration_token(access_token)

    if push_to_ssm:
        import boto3

        print("Trying to put params into ssm manager")
        client = boto3.client('ssm')
        client.put_parameter(
            Name=ssm_parameter_name,
            Value=runner_registration_token,
            Type='SecureString',
            Overwrite=True)
    else:
        print("Not push token to AWS Parameter Store, just print:", runner_registration_token)


def handler(event, context):
    private_key, app_id = get_key_and_app_from_aws()
    main(private_key, app_id, True, 'github_runner_registration_token')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get new token from github to add runners')
    parser.add_argument('-p', '--private-key-path', help='Path to file with private key')
    parser.add_argument('-k', '--private-key', help='Private key')
    parser.add_argument('-a', '--app-id', type=int, help='GitHub application ID', required=True)
    parser.add_argument('--push-to-ssm', action='store_true',  help='Store received token in parameter store')
    parser.add_argument('--ssm-parameter-name', default='github_runner_registration_token',  help='AWS paramater store parameter name')

    args = parser.parse_args()

    if not args.private_key_path and not args.private_key:
        print("Either --private-key-path or --private-key must be specified", file=sys.stderr)

    if args.private_key_path and args.private_key:
        print("Either --private-key-path or --private-key must be specified", file=sys.stderr)

    if args.private_key:
        private_key = args.private_key
    else:
        with open(args.private_key_path, 'r') as key_file:
            private_key = key_file.read()

    main(private_key, args.app_id, args.push_to_ssm, args.ssm_parameter_name)
