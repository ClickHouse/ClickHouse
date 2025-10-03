import pytest

from helpers.cluster import ClickHouseCluster
from .common import *


def start_clickhouse(config, err_msg):
    cluster = ClickHouseCluster(__file__)
    instance = cluster.add_instance("instance", main_configs=[config], with_vault=True)
    cluster.set_vault_startup_command(vault_startup_command)

    failed_to_start = False

    try:
        cluster.start()
    except Exception:
        failed_to_start = True

    assert failed_to_start

    message_found = instance.contains_in_log(err_msg, from_host=True)
    assert message_found


def test_wrong_url():
    start_clickhouse(
        "configs/config_wrong_url.xml",
        "DB::NetException: Not found address of host: wrong",
    )


def test_wrong_token():
    start_clickhouse(
        "configs/config_wrong_token.xml", "HTTP status code: 403 'Forbidden'"
    )


def test_wrong_secret():
    start_clickhouse(
        "configs/config_wrong_secret.xml", "HTTP status code: 404 'Not Found'"
    )


def test_wrong_key():
    start_clickhouse(
        "configs/config_wrong_key.xml",
        "DB::Exception: Key WRONG not found in secret ch_secret of vault",
    )
