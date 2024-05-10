#!/usr/bin/env python3
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import boto3  # type: ignore
from github import Github
from github.AuthenticatedUser import AuthenticatedUser


@dataclass
class Token:
    user: AuthenticatedUser
    value: str
    rest: int


def get_parameter_from_ssm(
    name: str, decrypt: bool = True, client: Optional[Any] = None
) -> str:
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")
    return client.get_parameter(  # type:ignore
        Name=name, WithDecryption=decrypt
    )[
        "Parameter"
    ]["Value"]


def get_parameters_from_ssm(
    names: List[str], decrypt: bool = True, client: Optional[Any] = None
) -> Dict[str, str]:
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")

    names = list(set(names))
    results = {}  # type: Dict[str,str]
    i = 0
    while (i) * 10 < len(names):
        # the get_parameters returns up to 10 values, so the call is split by 10
        results.update(
            **{
                p["Name"]: p["Value"]
                for p in client.get_parameters(
                    Names=names[i * 10 : (i + 1) * 10], WithDecryption=decrypt
                )["Parameters"]
            }
        )
        i += 1

    return results


ROBOT_TOKEN = None  # type: Optional[Token]


def get_best_robot_token(tokens_path: str = "/github-tokens") -> str:
    global ROBOT_TOKEN
    if ROBOT_TOKEN is not None:
        return ROBOT_TOKEN.value
    client = boto3.client("ssm", region_name="us-east-1")
    tokens = {
        p["Name"]: p["Value"]
        for p in client.get_parameters_by_path(Path=tokens_path, WithDecryption=True)[
            "Parameters"
        ]
    }
    assert tokens

    for value in tokens.values():
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
