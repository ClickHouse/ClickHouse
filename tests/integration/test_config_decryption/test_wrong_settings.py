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
    assert caught_exception.find(err_msg) != -1


def test_wrong_method():
    start_clickhouse("configs/config_wrong_method.xml", "Wrong encryption Method")


def test_invalid_chars():
    start_clickhouse(
        "configs/config_invalid_chars.xml",
        "Cannot read encrypted text, check for valid characters",
    )


def test_no_encryption_codecs():
    start_clickhouse(
        "configs/config_no_encryption_codecs.xml", "There is no key 0 in config"
    )


def test_subnodes():
    start_clickhouse("configs/config_subnodes.xml", "should have only one text node")
