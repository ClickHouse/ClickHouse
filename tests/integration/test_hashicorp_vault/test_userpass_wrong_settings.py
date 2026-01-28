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
    cluster.set_hashicorp_vault_startup_command(vault_startup_command_userpass)

    failed_to_start = False

    try:
        cluster.start()
    except Exception:
        failed_to_start = True

    assert failed_to_start

    message_found = instance.contains_in_log(err_msg, from_host=True)
    assert message_found


def test_missing_username():
    start_clickhouse(
        "configs/config_userpass_missing_username.xml",
        "configs/users.xml",
        "DB::Exception: username is not specified for vault",
    )


def test_empty_username():
    start_clickhouse(
        "configs/config_userpass_empty_username.xml",
        "configs/users.xml",
        "DB::Exception: username is not specified for vault",
    )


def test_userpass_empty():
    start_clickhouse(
        "configs/config_userpass_empty.xml",
        "configs/users.xml",
        "DB::Exception: username is not specified for vault",
    )


def test_userpass_with_token():
    start_clickhouse(
        "configs/config_userpass_with_token.xml",
        "configs/users.xml",
        "DB::Exception: Multiple auth methods are specified for vault",
    )


def test_wrong_username():
    start_clickhouse(
        "configs/config_userpass_wrong_username.xml",
        "configs/users.xml",
        "DB::Exception: Cannot login in vault as WRONG",
    )


def test_wrong_password():
    start_clickhouse(
        "configs/config_userpass_wrong_password.xml",
        "configs/users.xml",
        "DB::Exception: Cannot login in vault as user1",
    )
