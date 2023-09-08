#!/usr/bin/env python3
import logging
from dataclasses import dataclass
from typing import Optional

import boto3  # type: ignore
from github import Github
from github.AuthenticatedUser import AuthenticatedUser


@dataclass
class Token:
    user: AuthenticatedUser
    value: str
    rest: int


def get_parameter_from_ssm(name, decrypt=True, client=None):
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")
    return client.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]


ROBOT_TOKEN = None  # type: Optional[Token]


def get_best_robot_token(token_prefix_env_name="github_robot_token_"):
    global ROBOT_TOKEN
    if ROBOT_TOKEN is not None:
        return ROBOT_TOKEN.value
    client = boto3.client("ssm", region_name="us-east-1")
    parameters = client.describe_parameters(
        ParameterFilters=[
            {"Key": "Name", "Option": "BeginsWith", "Values": [token_prefix_env_name]}
        ]
    )["Parameters"]
    assert parameters

    for token_name in [p["Name"] for p in parameters]:
        value = get_parameter_from_ssm(token_name, True, client)
        gh = Github(value, per_page=100)
        # Do not spend additional request to API by accessin user.login unless
        # the token is chosen by the remaining requests number
        user = gh.get_user()
        rest, _ = gh.rate_limiting
        logging.info("Get token with %s remaining requests", rest)
        if ROBOT_TOKEN is None:
            ROBOT_TOKEN = Token(user, value, rest)
            continue
        if ROBOT_TOKEN.rest < rest:
            ROBOT_TOKEN.user, ROBOT_TOKEN.value, ROBOT_TOKEN.rest = user, value, rest

    assert ROBOT_TOKEN
    logging.info(
        "User %s with %s remaining requests is used",
        ROBOT_TOKEN.user.login,
        ROBOT_TOKEN.rest,
    )

    return ROBOT_TOKEN.value
