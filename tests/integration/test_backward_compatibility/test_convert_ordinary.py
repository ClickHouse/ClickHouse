import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, name="convert_ordinary")
node = cluster.add_instance(
    "node",
    image="yandex/clickhouse-server",
    tag="19.17.8.54",
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(query):
    return node.query(query, settings={"log_queries": 1})


def test_convert_system_db_to_atomic(start_cluster):
    q(
        "CREATE TABLE t(date Date, id UInt32) ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY id"
    )
    q("INSERT INTO t VALUES (today(), 1)")
    q("INSERT INTO t SELECT number % 1000, number FROM system.numbers LIMIT 1000000")

    assert "1000001\n" == q("SELECT count() FROM t")
    assert "499999500001\n" == q("SELECT sum(id) FROM t")
    assert "1970-01-01\t1000\t499500000\n1970-01-02\t1000\t499501000\n" == q(
        "SELECT date, count(), sum(id) FROM t GROUP BY date ORDER BY date LIMIT 2"
    )
    q("SYSTEM FLUSH LOGS")

    assert "query_log" in q("SHOW TABLES FROM system")
    assert "part_log" in q("SHOW TABLES FROM system")
    q("SYSTEM FLUSH LOGS")
    assert "1\n" == q("SELECT count() != 0 FROM system.query_log")
    assert "1\n" == q("SELECT count() != 0 FROM system.part_log")

    node.restart_with_latest_version(fix_metadata=True)

    assert "Ordinary" in node.query("SHOW CREATE DATABASE default")
    assert "Atomic" in node.query("SHOW CREATE DATABASE system")
    assert "query_log" in node.query("SHOW TABLES FROM system")
    assert "part_log" in node.query("SHOW TABLES FROM system")
    node.query("SYSTEM FLUSH LOGS")

    assert "query_log_0" in node.query("SHOW TABLES FROM system")
    assert "part_log_0" in node.query("SHOW TABLES FROM system")
    assert "1\n" == node.query("SELECT count() != 0 FROM system.query_log_0")
    assert "1\n" == node.query("SELECT count() != 0 FROM system.part_log_0")
    assert "1970-01-01\t1000\t499500000\n1970-01-02\t1000\t499501000\n" == node.query(
        "SELECT date, count(), sum(id) FROM t GROUP BY date ORDER BY date LIMIT 2"
    )
    assert "INFORMATION_SCHEMA\ndefault\ninformation_schema\nsystem\n" == node.query(
        "SELECT name FROM system.databases ORDER BY name"
    )

    errors_count = node.count_in_log("<Error>")
    assert "0\n" == errors_count or (
        "1\n" == errors_count
        and "1\n" == node.count_in_log("Can't receive Netlink response")
    )
    assert "0\n" == node.count_in_log("<Warning> Database")
    errors_count = node.count_in_log("always include the lines below")
    assert "0\n" == errors_count or (
        "1\n" == errors_count
        and "1\n" == node.count_in_log("Can't receive Netlink response")
    )
