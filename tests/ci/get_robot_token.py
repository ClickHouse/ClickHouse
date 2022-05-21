#!/usr/bin/env python3
import boto3  # type: ignore
from github import Github  # type: ignore


def get_parameter_from_ssm(name, decrypt=True, client=None):
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")
    return client.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]


def get_best_robot_token(token_prefix_env_name="github_robot_token_", total_tokens=4):
    client = boto3.client("ssm", region_name="us-east-1")
    tokens = {}
    for i in range(1, total_tokens + 1):
        token_name = token_prefix_env_name + str(i)
        token = get_parameter_from_ssm(token_name, True, client)
        gh = Github(token)
        rest, _ = gh.rate_limiting
        tokens[token] = rest

    return max(tokens.items(), key=lambda x: x[1])[0]
