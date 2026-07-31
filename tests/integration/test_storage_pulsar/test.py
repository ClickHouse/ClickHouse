import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    user_configs=["configs/users.xml"],
    with_pulsar=True,
)


@pytest.fixture(scope="module")
def pulsar_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_tables():
    yield
    instance.query("DROP TABLE IF EXISTS test.view SYNC")
    instance.query("DROP TABLE IF EXISTS test.consumer SYNC")
    instance.query("DROP TABLE IF EXISTS test.pulsar_writer SYNC")
    instance.query("DROP TABLE IF EXISTS test.pulsar_reader SYNC")


def pulsar_table(name, topic, group, extra_settings=""):
    return f"""
        CREATE TABLE {name} (key UInt64, value UInt64)
        ENGINE = Pulsar
        SETTINGS pulsar_service_url = 'pulsar://pulsar1:6650',
                 pulsar_topic_list = '{topic}',
                 pulsar_group_name = '{group}',
                 pulsar_format = 'JSONEachRow'{extra_settings}
    """


def wait_query_result(expected, query, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = instance.query(query)
        if TSV(result) == TSV(expected):
            return
        time.sleep(1)
    assert TSV(instance.query(query)) == TSV(expected)


def test_experimental_gate(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    error = instance.query_and_get_error(
        pulsar_table("test.pulsar_reader", "gate_topic", "gate_group"),
        settings={"allow_experimental_pulsar_storage_engine": 0},
    )
    assert "SUPPORT_IS_DISABLED" in error


def test_direct_select_requires_setting(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "select_gate_topic", "select_gate_group"))
    error = instance.query_and_get_error(
        "SELECT * FROM test.pulsar_reader",
        settings={"stream_like_engine_allow_direct_select": 0},
    )
    assert "QUERY_NOT_ALLOWED" in error


def test_produce_consume_via_materialized_view(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query(pulsar_table("test.pulsar_reader", "mv_topic", "mv_group"))
    instance.query(pulsar_table("test.pulsar_writer", "mv_topic", "writer_group"))
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT key, value FROM test.pulsar_reader
        """
    )

    num_rows = 50
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number * number FROM numbers({num_rows})"
    )

    expected = "\n".join(f"{i}\t{i * i}" for i in range(num_rows))
    wait_query_result(expected, "SELECT key, value FROM test.view ORDER BY key")


def test_direct_select(pulsar_cluster):
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    # The subscription is created together with the table, so only messages
    # published after this point are delivered to it.
    instance.query(
        pulsar_table(
            "test.pulsar_reader",
            "select_topic",
            "select_group",
            extra_settings=", pulsar_commit_on_select = 1",
        )
    )
    instance.query(pulsar_table("test.pulsar_writer", "select_topic", "select_writer_group"))

    num_rows = 20
    instance.query(
        f"INSERT INTO test.pulsar_writer SELECT number, number FROM numbers({num_rows})"
    )

    # A direct SELECT reads at most one batch per consumer, so accumulate the
    # rows over multiple queries. With `pulsar_commit_on_select = 1` returned
    # messages are acknowledged, so every row is seen at least once and
    # duplicates are possible only on redelivery.
    seen = set()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and len(seen) < num_rows:
        result = instance.query("SELECT key, value FROM test.pulsar_reader")
        for line in result.strip().splitlines():
            seen.add(line)
        time.sleep(0.2)
    expected = {f"{i}\t{i}" for i in range(num_rows)}
    assert seen == expected
