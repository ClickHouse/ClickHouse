import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_check_timezone_config(start_cluster):
    assert node.query("SELECT toDateTime(1111111111)") == "2005-03-17 17:58:31\n"


def test_overflow_toDate(start_cluster):
    assert node.query("SELECT toDate('2999-12-31','UTC')") == "2149-06-06\n"
    assert node.query("SELECT toDate('2021-12-21','UTC')") == "2021-12-21\n"
    assert node.query("SELECT toDate('1000-12-31','UTC')") == "1970-01-01\n"


def test_overflow_toDate32(start_cluster):
    assert node.query("SELECT toDate32('2999-12-31','UTC')") == "2283-11-11\n"
    assert node.query("SELECT toDate32('2021-12-21','UTC')") == "2021-12-21\n"
    assert node.query("SELECT toDate32('1000-12-31','UTC')") == "1925-01-01\n"


def test_overflow_toDateTime(start_cluster):
    assert (
        node.query("SELECT toDateTime('2999-12-31 00:00:00','UTC')")
        == "2106-02-07 06:28:15\n"
    )
    assert (
        node.query("SELECT toDateTime('2106-02-07 06:28:15','UTC')")
        == "2106-02-07 06:28:15\n"
    )
    assert (
        node.query("SELECT toDateTime('1970-01-01 00:00:00','UTC')")
        == "1970-01-01 00:00:00\n"
    )
    assert (
        node.query("SELECT toDateTime('1000-01-01 00:00:00','UTC')")
        == "1970-01-01 00:00:00\n"
    )


def test_overflow_parseDateTimeBestEffort(start_cluster):
    assert (
        node.query("SELECT parseDateTimeBestEffort('2999-12-31 00:00:00','UTC')")
        == "2106-02-07 06:28:15\n"
    )
    assert (
        node.query("SELECT parseDateTimeBestEffort('2106-02-07 06:28:15','UTC')")
        == "2106-02-07 06:28:15\n"
    )
    assert (
        node.query("SELECT parseDateTimeBestEffort('1970-01-01 00:00:00','UTC')")
        == "1970-01-01 00:00:00\n"
    )
    assert (
        node.query("SELECT parseDateTimeBestEffort('1000-01-01 00:00:00','UTC')")
        == "1970-01-01 00:00:00\n"
    )
