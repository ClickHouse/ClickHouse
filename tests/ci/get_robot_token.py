#!/usr/bin/env python3
import logging
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import boto3  # type: ignore
from github import Github
from github.AuthenticatedUser import AuthenticatedUser
from github.GithubException import BadCredentialsException
from github.NamedUser import NamedUser


@dataclass
class Token:
    user: Union[AuthenticatedUser, NamedUser]
    value: str
    rest: int


SAFE_REQUESTS_LIMIT = 1000


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
    global ROBOT_TOKEN  # pylint:disable=global-statement
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

    token_items = list(tokens.items())
    random.shuffle(token_items)

    best_token: Optional[Token] = None

    for name, value in token_items:
        gh = Github(value, per_page=100)
        try:
            # Do not spend additional request to API by accessing user.login unless
            # the token is chosen by the remaining requests number
            user = gh.get_user()
            rest, _ = gh.rate_limiting
        except BadCredentialsException:
            logging.error(
                "The token %(name)s has expired, please update it\n"
                "::error::Token %(name)s has expired, it must be updated",
                {"name": name},
            )
            continue
        logging.info("Get token with %s remaining requests", rest)
        if best_token is None:
            best_token = Token(user, value, rest)
        elif best_token.rest < rest:
            best_token = Token(user, value, rest)
        if best_token.rest > SAFE_REQUESTS_LIMIT:
            break
    assert best_token
    ROBOT_TOKEN = best_token
    logging.info(
        "User %s with %s remaining requests is used",
        ROBOT_TOKEN.user.login,
        ROBOT_TOKEN.rest,
    )

    return ROBOT_TOKEN.value
