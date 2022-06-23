#!/usr/bin/env python3
import boto3  # type: ignore
import os
from github import Github  # type: ignore


def get_parameter_from_ssm(name, decrypt=True, client=None, use_env=False):
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")
    parameter = ''
    if use_env:
        parameter = os.getenv(name)
    else:
        parameter = client.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]
    return parameter

def get_best_robot_token(token_prefix_env_name="github_robot_token_", total_tokens=4, use_env=False):
    client = boto3.client("ssm", region_name="us-east-1")
    tokens = {}
    for i in range(1, total_tokens + 1):
        token_name = token_prefix_env_name + str(i)
        token = get_parameter_from_ssm(token_name, True, client, use_env)
        gh = Github(token)
        rest, _ = gh.rate_limiting
        tokens[token] = rest

    return max(tokens.items(), key=lambda x: x[1])[0]
