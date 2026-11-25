import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)

# Read the data through `ReadBufferFromFileDescriptor`, where the failpoint corrupts it, bypassing every cache.
BUFFERED_READ_SETTINGS = {
    "local_filesystem_read_method": "pread",
    "max_threads": 1,
    "use_uncompressed_cache": 0,
    "use_page_cache_for_local_disks": 0,
    "min_bytes_to_use_direct_io": 0,
}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


PASSED_AFTER_PAGE_CACHE_READ_ERROR = (
    "SELECT sum(value) FROM system.events WHERE event = 'PartCheckPassedAfterPageCacheReadError'"
)


@pytest.mark.parametrize(
    "table, columns, values",
    [
        ("t", "x UInt64, s String", "number, toString(number)"),
        # A Wide part without `columns_substreams.txt` (written before 25.8) with a column of dynamic structure.
        ("t_json", "x UInt64, s String, j JSON", "number, toString(number), toJSONString(map('a', number))"),
    ],
)
def test_part_check_after_read_error_reads_with_direct_io(started_cluster, table, columns, values):
    node.query(
        f"""
        CREATE TABLE {table} ({columns})
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', 'r1') ORDER BY x
        SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0
        """
    )
    node.query(f"INSERT INTO {table} SELECT {values} FROM numbers(100000)")
    if table == "t_json":
        path = node.query(f"SELECT path FROM system.parts WHERE table = '{table}' AND active").strip()
        node.exec_in_container(["rm", f"{path}/columns_substreams.txt"])
        node.query(f"DETACH TABLE {table}")
        node.query(f"ATTACH TABLE {table}")
    node.query(f"SYSTEM STOP MERGES {table}")

    node.query(f"CHECK TABLE {table}", query_id=f"check_part_direct_io_{table}")
    node.query("SYSTEM FLUSH LOGS query_log")
    direct_io_failed = node.query(
        "SELECT ProfileEvents['CreatedReadBufferDirectIOFailed'] FROM system.query_log "
        f"WHERE query_id = 'check_part_direct_io_{table}' AND type = 'QueryFinish'"
    )
    if int(direct_io_failed) > 0:
        pytest.skip("The filesystem does not support O_DIRECT")

    # Loads the marks, so that the failing read below is a read of the column data.
    query = f"SELECT sum(x), sum(length(s)) FROM {table}"
    expected = node.query(query, settings=BUFFERED_READ_SETTINGS)
    passed_before = int(node.query(PASSED_AFTER_PAGE_CACHE_READ_ERROR))

    node.query("SYSTEM ENABLE FAILPOINT file_read_corrupt_data_without_direct_io")
    try:
        with pytest.raises(QueryRuntimeException, match="CHECKSUM_DOESNT_MATCH"):
            node.query(query, settings=BUFFERED_READ_SETTINGS)

        assert_eq_with_retry(node, PASSED_AFTER_PAGE_CACHE_READ_ERROR, str(passed_before + 1))
    finally:
        node.query("SYSTEM DISABLE FAILPOINT file_read_corrupt_data_without_direct_io")

    assert node.query(f"SELECT name FROM system.parts WHERE table = '{table}' AND active") == "all_0_0_0\n"
    assert node.query(f"SELECT count() FROM system.detached_parts WHERE table = '{table}'") == "0\n"
    assert (
        node.query(
            "SELECT count() FROM system.zookeeper "
            f"WHERE path = '/clickhouse/tables/{table}/replicas/r1/parts' AND name = 'all_0_0_0'"
        )
        == "1\n"
    )
    assert node.contains_in_log(
        f"default.{table} (ReplicatedMergeTreePartCheckThread): "
        "Part all_0_0_0 was checked after a read error and is fine when read with O_DIRECT"
    )
    assert node.query(query, settings=BUFFERED_READ_SETTINGS) == expected
