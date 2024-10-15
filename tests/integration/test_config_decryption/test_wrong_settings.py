import pytest

from helpers.cluster import ClickHouseCluster


def start_clickhouse(config, err_msg):
    cluster = ClickHouseCluster(__file__)
    node = cluster.add_instance("node", main_configs=[config])
    caught_exception = ""
    try:
        cluster.start()
    except Exception as e:
        caught_exception = str(e)
    assert err_msg in caught_exception


def test_wrong_method():
    start_clickhouse(
        "configs/config_wrong_method.xml", "Unknown encryption method. Got WRONG"
    )


def test_invalid_chars():
    start_clickhouse(
        "configs/config_invalid_chars.xml",
        "Cannot read encrypted text, check for valid characters",
    )


def test_no_encryption_key():
    start_clickhouse(
        "configs/config_no_encryption_key.xml",
        "There is no key 0 in config for AES_128_GCM_SIV encryption codec",
    )


def test_subnodes():
    start_clickhouse("configs/config_subnodes.xml", "cannot contain nested elements")
