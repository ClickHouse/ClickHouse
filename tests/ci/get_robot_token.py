#!/usr/bin/env python3
import os
from github import Github

def get_best_robot_token(token_prefix_env_name="ROBOT_TOKEN_", total_tokens=4):
    tokens = {}
    for i in range(total_tokens):
        token_name = token_prefix_env_name + str(i)
        token = os.getenv(token_name)
        gh = Github(token)
        rest, _ = gh.rate_limiting
        tokens[token] = rest

    return max(tokens.items(), key=lambda x: x[1])[0]
