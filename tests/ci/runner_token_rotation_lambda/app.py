#!/usr/bin/env python3

import argparse
import sys

import boto3  # type: ignore
import requests
from lambda_shared.token import get_access_token_by_key_app, get_cached_access_token


def get_runner_registration_token(access_token):
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.post(
        "https://api.github.com/orgs/ClickHouse/actions/runners/registration-token",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data["token"]


def main(access_token, push_to_ssm, ssm_parameter_name):
    runner_registration_token = get_runner_registration_token(access_token)

    if push_to_ssm:
        print("Trying to put params into ssm manager")
        client = boto3.client("ssm")
        client.put_parameter(
            Name=ssm_parameter_name,
            Value=runner_registration_token,
            Type="SecureString",
            Overwrite=True,
        )
    else:
        print(
            "Not push token to AWS Parameter Store, just print:",
            runner_registration_token,
        )


def handler(event, context):
    _, _ = event, context
    main(get_cached_access_token(), True, "github_runner_registration_token")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get new token from github to add runners"
    )
    parser.add_argument(
        "-p", "--private-key-path", help="Path to file with private key"
    )
    parser.add_argument("-k", "--private-key", help="Private key")
    parser.add_argument(
        "-a", "--app-id", type=int, help="GitHub application ID", required=True
    )
    parser.add_argument(
        "--push-to-ssm",
        action="store_true",
        help="Store received token in parameter store",
    )
    parser.add_argument(
        "--ssm-parameter-name",
        default="github_runner_registration_token",
        help="AWS paramater store parameter name",
    )

    args = parser.parse_args()

    if not args.private_key_path and not args.private_key:
        print(
            "Either --private-key-path or --private-key must be specified",
            file=sys.stderr,
        )

    if args.private_key_path and args.private_key:
        print(
            "Either --private-key-path or --private-key must be specified",
            file=sys.stderr,
        )

    if args.private_key:
        private_key = args.private_key
    else:
        with open(args.private_key_path, "r", encoding="utf-8") as key_file:
            private_key = key_file.read()

    token = get_access_token_by_key_app(private_key, args.app_id)
    main(token, args.push_to_ssm, args.ssm_parameter_name)
