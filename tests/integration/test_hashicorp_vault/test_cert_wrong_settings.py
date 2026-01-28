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
    cluster.set_hashicorp_vault_startup_command(vault_startup_command_cert)

    failed_to_start = False

    try:
        cluster.start()
    except Exception:
        failed_to_start = True

    assert failed_to_start

    message_found = instance.contains_in_log(err_msg, from_host=True)
    assert message_found


def test_missing_ssl():
    start_clickhouse(
        "configs/config_cert_missing_ssl.xml",
        "configs/users.xml",
        "DB::Exception: ssl section is not specified for vault",
    )


def test_empty_ssl():
    start_clickhouse(
        "configs/config_cert_empty_ssl.xml",
        "configs/users.xml",
        "DB::Exception: privateKeyFile is not specified for vault",
    )

def test_empty_private_key():
    start_clickhouse(
        "configs/config_cert_empty_private_key.xml",
        "configs/users.xml",
        "DB::Exception: privateKeyFile is not specified for vault",
    )

def test_missing_certificate():
    start_clickhouse(
        "configs/config_cert_missing_certificate.xml",
        "configs/users.xml",
        "DB::Exception: certificateFile is not specified for vault",
    )

def test_empty_certificate():
    start_clickhouse(
        "configs/config_cert_empty_certificate.xml",
        "configs/users.xml",
        "DB::Exception: certificateFile is not specified for vault",
    )
