"""Module to get the token for GitHub"""
from dataclasses import dataclass
import json
import time
from typing import Tuple

import boto3  # type: ignore
import jwt
import requests

from . import cached_value_is_valid


def get_key_and_app_from_aws() -> Tuple[str, int]:
    secret_name = "clickhouse_github_secret_key"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    data = json.loads(get_secret_value_response["SecretString"])
    return data["clickhouse-app-key"], int(data["clickhouse-app-id"])


def get_installation_id(jwt_token: str) -> int:
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get("https://api.github.com/app/installations", headers=headers)
    response.raise_for_status()
    data = response.json()
    for installation in data:
        if installation["account"]["login"] == "ClickHouse":
            installation_id = installation["id"]

    return installation_id  # type: ignore


def get_access_token_by_jwt(jwt_token: str, installation_id: int) -> str:
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.post(
        f"https://api.github.com/app/installations/{installation_id}/access_tokens",
        headers=headers,
    )
    response.raise_for_status()
    data = response.json()
    return data["token"]  # type: ignore


def get_token_from_aws() -> str:
    private_key, app_id = get_key_and_app_from_aws()
    return get_access_token_by_key_app(private_key, app_id)


def get_access_token_by_key_app(private_key: str, app_id: int) -> str:
    payload = {
        "iat": int(time.time()) - 60,
        "exp": int(time.time()) + (10 * 60),
        "iss": app_id,
    }

    encoded_jwt = jwt.encode(payload, private_key, algorithm="RS256")
    installation_id = get_installation_id(encoded_jwt)
    return get_access_token_by_jwt(encoded_jwt, installation_id)


@dataclass
class CachedToken:
    time: float
    value: str
    updating: bool = False


_cached_token = CachedToken(0, "")


def get_cached_access_token() -> str:
    if time.time() - 550 < _cached_token.time or _cached_token.updating:
        return _cached_token.value
    # Indicate that the value is updating now, so the cached value can be
    # used. The first setting and close-to-ttl are not counted as update
    _cached_token.updating = cached_value_is_valid(_cached_token.time, 590)
    private_key, app_id = get_key_and_app_from_aws()
    _cached_token.time = time.time()
    _cached_token.value = get_access_token_by_key_app(private_key, app_id)
    _cached_token.updating = False
    return _cached_token.value
