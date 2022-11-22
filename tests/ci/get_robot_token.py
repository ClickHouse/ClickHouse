#!/usr/bin/env python3
import logging

import boto3  # type: ignore
from github import Github  # type: ignore


def get_parameter_from_ssm(name, decrypt=True, client=None):
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")
    return client.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]


def get_best_robot_token(token_prefix_env_name="github_robot_token_"):
    client = boto3.client("ssm", region_name="us-east-1")
    parameters = client.describe_parameters(
        ParameterFilters=[
            {"Key": "Name", "Option": "BeginsWith", "Values": [token_prefix_env_name]}
        ]
    )["Parameters"]
    assert parameters
    token = {"login": "", "value": "", "rest": 0}

    for token_name in [p["Name"] for p in parameters]:
        value = get_parameter_from_ssm(token_name, True, client)
        gh = Github(value, per_page=100)
        login = gh.get_user().login
        rest, _ = gh.rate_limiting
        logging.info("Get token for user %s with %s remaining requests", login, rest)
        if token["rest"] < rest:
            token = {"login": login, "value": value, "rest": rest}

    assert token["login"]
    logging.info(
        "User %s with %s remaining requests is used", token["login"], token["rest"]
    )

    return token["value"]
