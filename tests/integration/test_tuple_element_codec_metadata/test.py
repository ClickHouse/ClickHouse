import json

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True)

TABLE = "tuple_element_codec_metadata"
KEEPER_PATH = f"/clickhouse/tables/test/{TABLE}"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_replica(node, replica):
    node.query(
        f"""
        CREATE TABLE {TABLE}
        (
            key UInt64,
            payload Tuple(overridden UInt64, inherited String) CODEC(LZ4)
        )
        ENGINE = ReplicatedMergeTree('{KEEPER_PATH}', '{replica}')
        ORDER BY key
        SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0
        """
    )


def show_create(node):
    return node.query_with_retry(
        f"SHOW CREATE TABLE {TABLE}",
        check_callback=lambda result: "overridden UInt64 CODEC(NONE)" in result,
    )


def effective_codecs(node):
    rows = node.query(
        f"DESCRIBE TABLE {TABLE} FORMAT JSONEachRow "
        "SETTINGS describe_include_subcolumns = 1"
    )
    return {
        row["name"]: row["codec_expression"]
        for row in map(json.loads, rows.splitlines())
    }


def assert_replayed_policy(node):
    create_query = show_create(node)
    assert "overridden UInt64 CODEC(NONE)" in create_query
    assert ") CODEC(LZ4)" in create_query

    codecs = effective_codecs(node)
    assert codecs["payload.overridden"] == "NONE"
    assert codecs["payload.inherited"] == "LZ4"


def test_keeper_columns_format_v2_round_trip(started_cluster):
    create_replica(node1, "replica1")
    create_replica(node2, "replica2")

    zk = started_cluster.get_kazoo_client("zoo1")
    root_only_columns = zk.get(f"{KEEPER_PATH}/columns")[0].decode()
    assert root_only_columns.startswith("columns format version: 1\n")
    assert "TUPLE_ELEMENT_CODECS" not in root_only_columns

    node1.query(
        f"""
        ALTER TABLE {TABLE} MODIFY COLUMN payload
            Tuple(overridden UInt64 CODEC(NONE), inherited String) CODEC(LZ4)
        """,
        settings={"enable_tuple_element_codecs": 1},
    )

    columns = zk.get(f"{KEEPER_PATH}/columns")[0].decode()
    assert columns.startswith("columns format version: 2\n")
    payload_line = next(
        line for line in columns.splitlines() if line.startswith("`payload` ")
    )
    assert "TUPLE_ELEMENT_CODECS" in payload_line
    assert "Tuple(overridden UInt64, inherited String)" in payload_line
    assert "Tuple(overridden UInt64 CODEC" not in payload_line
    assert "CODEC(NONE)" in payload_line
    assert payload_line.endswith("\tCODEC(LZ4)")

    assert node2.query(
        "SELECT value FROM system.settings "
        "WHERE name = 'enable_tuple_element_codecs'"
    ) == "0\n"
    assert_replayed_policy(node2)

    node2.query(
        f"""
        INSERT INTO {TABLE}
        SELECT number, (number * 3, concat('value-', toString(number % 10)))
        FROM numbers(10000)
        """
    )
    assert node2.query(
        f"""
        SELECT mapContains(codec_block_counts, 'NONE')
        FROM mergeTreeCodecBlockCounts(currentDatabase(), '{TABLE}')
        WHERE column = 'payload' AND substream = 'payload%2Eoverridden'
        """
    ) == "1\n"

    node1.query(f"SYSTEM SYNC REPLICA {TABLE}", timeout=60)
    assert node1.query(f"SELECT count(), sum(key) FROM {TABLE}") == (
        "10000\t49995000\n"
    )

    node2.restart_clickhouse()
    assert_replayed_policy(node2)
    assert node2.query(f"SELECT count(), sum(key) FROM {TABLE}") == (
        "10000\t49995000\n"
    )
