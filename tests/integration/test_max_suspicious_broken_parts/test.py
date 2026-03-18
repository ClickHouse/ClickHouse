# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def break_part(table, part_name):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"rm /var/lib/clickhouse/data/default/{table}/{part_name}/primary.cidx",
        ]
    )


def remove_part(table, part_name):
    node.exec_in_container(
        ["bash", "-c", f"rm -r /var/lib/clickhouse/data/default/{table}/{part_name}"]
    )


def get_count(table):
    return int(node.query(f"SELECT count() FROM {table}").strip())


def detach_table(table):
    node.query(f"DETACH TABLE {table}")


def attach_table(table):
    node.query(f"ATTACH TABLE {table}")


def check_table(table):
    rows = 900
    per_part_rows = 90

    node.query(f"INSERT INTO {table} SELECT * FROM numbers(900)")

    assert get_count(table) == rows

    # break one part, and check that clickhouse will be alive
    break_part(table, "0_1_1_0")
    rows -= per_part_rows
    detach_table(table)
    attach_table(table)
    assert get_count(table) == rows

    # break two parts, and check that clickhouse will not start
    break_part(table, "1_2_2_0")
    break_part(table, "2_3_3_0")
    rows -= per_part_rows * 2
    detach_table(table)
    with pytest.raises(QueryRuntimeException):
        attach_table(table)

    # now remove one part, and check
    remove_part(table, "1_2_2_0")
    attach_table(table)
    assert get_count(table) == rows

    node.query(f"DROP TABLE {table}")


def test_max_suspicious_broken_parts():
    node.query(
        """
    CREATE TABLE test_max_suspicious_broken_parts (
        key Int
    )
    ENGINE=MergeTree
    ORDER BY key
    PARTITION BY key%10
    SETTINGS
        max_suspicious_broken_parts = 1;
    """
    )
    check_table("test_max_suspicious_broken_parts")


def test_max_suspicious_broken_parts_bytes():
    node.query(
        """
    CREATE TABLE test_max_suspicious_broken_parts_bytes (
        key Int
    )
    ENGINE=MergeTree
    ORDER BY key
    PARTITION BY key%10
    SETTINGS
        max_suspicious_broken_parts = 10,
        /* one part takes ~751 byte, so we allow failure of one part with these limit */
        max_suspicious_broken_parts_bytes = 1000;
    """
    )
    check_table("test_max_suspicious_broken_parts_bytes")


def test_max_suspicious_broken_parts__wide():
    node.query(
        """
    CREATE TABLE test_max_suspicious_broken_parts__wide (
        key Int
    )
    ENGINE=MergeTree
    ORDER BY key
    PARTITION BY key%10
    SETTINGS
        min_bytes_for_wide_part = 0,
        max_suspicious_broken_parts = 1;
    """
    )
    check_table("test_max_suspicious_broken_parts__wide")


def test_max_suspicious_broken_parts_bytes__wide():
    node.query(
        """
    CREATE TABLE test_max_suspicious_broken_parts_bytes__wide (
        key Int
    )
    ENGINE=MergeTree
    ORDER BY key
    PARTITION BY key%10
    SETTINGS
        min_bytes_for_wide_part = 0,
        max_suspicious_broken_parts = 10,
        /* one part takes ~750 byte, so we allow failure of one part with these limit */
        max_suspicious_broken_parts_bytes = 1000;
    """
    )
    check_table("test_max_suspicious_broken_parts_bytes__wide")
