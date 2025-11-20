import pytest

from helpers.cluster import ClickHouseCluster
from .common import *


def start_clickhouse(config, users, err_msg):
    cluster = ClickHouseCluster(__file__)
    instance = cluster.add_instance(
        "instance",
        main_configs=[config],
        user_configs=[users],
        with_hashicorp_vault=True,
    )
    cluster.set_hashicorp_vault_startup_command(vault_startup_command)

    failed_to_start = False

    try:
        cluster.start()
    except Exception:
        failed_to_start = True

    assert failed_to_start

    message_found = instance.contains_in_log(err_msg, from_host=True)
    assert message_found


def test_missing_url():
    start_clickhouse(
        "configs/config_missing_url.xml",
        "configs/users.xml",
        "DB::Exception: url is not specified for vault",
    )


def test_wrong_url():
    start_clickhouse(
        "configs/config_wrong_url.xml",
        "configs/users.xml",
        "DB::Exception: Poco::Exception. Code: 1000",
    )


def test_empty_url():
    start_clickhouse(
        "configs/config_empty_url.xml",
        "configs/users.xml",
        "DB::Exception: url is not specified for vault",
    )


def test_missing_auth_info():
    start_clickhouse(
        "configs/config_missing_auth_info.xml",
        "configs/users.xml",
        "DB::Exception: Auth sections are not specified for vault",
    )


def test_token_wrong_token():
    start_clickhouse(
        "configs/config_token_wrong_token.xml",
        "configs/users.xml",
        "Exception: HTTP error: 403",
    )


def test_token_empty_token():
    start_clickhouse(
        "configs/config_token_empty_token.xml",
        "configs/users.xml",
        "DB::Exception: token is not specified for vault",
    )


def test_wrong_secret():
    start_clickhouse(
        "configs/config_token.xml",
        "configs/users_wrong_secret.xml",
        "Exception: HTTP error: 404",
    )


def test_wrong_key():
    start_clickhouse(
        "configs/config_token.xml",
        "configs/users_wrong_key.xml",
        "DB::Exception: Key WRONG not found in secret username of vault",
    )
