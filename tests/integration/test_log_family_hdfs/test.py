import logging
import time

import pytest
from pyhdfs import HdfsClient

from helpers.cluster import ClickHouseCluster, is_arm

if is_arm():
    pytestmark = pytest.mark.skip


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node", main_configs=["configs/storage_conf.xml"], with_hdfs=True
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")
        fs.mkdirs("/clickhouse")

        yield cluster
    finally:
        cluster.shutdown()


def count_hdfs_objects(fs, path="/clickhouse"):
    # The object keys contain a nested directory prefix (e.g. `abc/xyz...`),
    # so count the files recursively; directories are not objects.
    return sum(len(files) for _, _, files in fs.walk(path))


def hdfs_directories(fs, path="/clickhouse"):
    return {root + "/" + name for root, dirs, _ in fs.walk(path) for name in dirs}


def assert_objects_count(started_cluster, objects_count, num_tries=30):
    # Removal of blobs is asynchronous, so wait until the count converges.
    fs = HdfsClient(hosts=started_cluster.hdfs_ip, user_name="root")
    while num_tries > 0:
        if objects_count == count_hdfs_objects(fs):
            break
        num_tries -= 1
        time.sleep(1)
    assert objects_count == count_hdfs_objects(fs)


# TinyLog: files: id.bin, sizes.json
# INSERT overwrites 1 file (`sizes.json`) and appends 1 file (`id.bin`), so
# files_overhead=1, files_overhead_per_insert=1
#
# Log: files: id.bin, __marks.mrk, sizes.json
# INSERT overwrites 1 file (`sizes.json`), and appends 2 files (`id.bin`, `__marks.mrk`), so
# files_overhead=1, files_overhead_per_insert=2
#
# StripeLog: files: data.bin, index.mrk, sizes.json
# INSERT overwrites 1 file (`sizes.json`), and appends 2 files (`index.mrk`, `data.bin`), so
# files_overhead=1, files_overhead_per_insert=2
@pytest.mark.parametrize(
    "log_engine,files_overhead,files_overhead_per_insert",
    [("TinyLog", 1, 1), ("Log", 1, 2), ("StripeLog", 1, 2)],
)
def test_log_family_hdfs(
    started_cluster, log_engine, files_overhead, files_overhead_per_insert
):
    node = started_cluster.instances["node"]

    node.query(
        "CREATE TABLE hdfs_test (id UInt64) ENGINE={} SETTINGS disk = 'hdfs'".format(
            log_engine
        )
    )

    try:
        node.query("INSERT INTO hdfs_test SELECT number FROM numbers(5)")
        assert node.query("SELECT * FROM hdfs_test") == "0\n1\n2\n3\n4\n"
        assert_objects_count(
            started_cluster, files_overhead_per_insert + files_overhead
        )

        node.query("INSERT INTO hdfs_test SELECT number + 5 FROM numbers(3)")
        assert (
            node.query("SELECT * FROM hdfs_test order by id")
            == "0\n1\n2\n3\n4\n5\n6\n7\n"
        )
        assert_objects_count(
            started_cluster, files_overhead_per_insert * 2 + files_overhead
        )

        node.query("INSERT INTO hdfs_test SELECT number + 8 FROM numbers(1)")
        assert (
            node.query("SELECT * FROM hdfs_test order by id")
            == "0\n1\n2\n3\n4\n5\n6\n7\n8\n"
        )
        assert_objects_count(
            started_cluster, files_overhead_per_insert * 3 + files_overhead
        )

        node.query("TRUNCATE TABLE hdfs_test")
        assert_objects_count(started_cluster, 0)
    finally:
        node.query("DROP TABLE hdfs_test SYNC")


def test_prefix_directories_are_not_reclaimed(started_cluster):
    # An emptied object-key prefix directory is deliberately kept: deleting it
    # races a concurrent write that has just created the same directory and has
    # not opened its object yet, which fails that write with
    # `Parent directory doesn't exist`.
    node = started_cluster.instances["node"]
    fs = HdfsClient(hosts=started_cluster.hdfs_ip, user_name="root")

    node.query(
        "CREATE TABLE hdfs_dir_cleanup (id UInt64) ENGINE=TinyLog SETTINGS disk = 'hdfs'"
    )
    try:
        node.query("INSERT INTO hdfs_dir_cleanup SELECT number FROM numbers(5)")
        assert count_hdfs_objects(fs) > 0
        directories_before = hdfs_directories(fs)
        assert directories_before

        node.query("TRUNCATE TABLE hdfs_dir_cleanup")
        assert_objects_count(started_cluster, 0)
        # Removing the objects must not remove their prefix directories.
        assert hdfs_directories(fs) >= directories_before
    finally:
        node.query("DROP TABLE hdfs_dir_cleanup SYNC")


def test_no_leftover_files_after_canceled_write(started_cluster):
    # A canceled write (e.g. an INSERT that fails mid-stream, or a zero-row
    # rewrite of a part) creates its HDFS file already at open, so it has to
    # remove that file explicitly.
    node = started_cluster.instances["node"]
    fs = HdfsClient(hosts=started_cluster.hdfs_ip, user_name="root")

    node.query(
        "CREATE TABLE hdfs_canceled_write (id UInt64) ENGINE=TinyLog SETTINGS disk = 'hdfs'"
    )
    try:
        # Establish a baseline: `id.bin` and `sizes.json`. The count cannot be
        # zero even after a canceled insert, because the sink saves the file
        # sizes (`sizes.json`) upfront to be able to roll back on error.
        node.query("INSERT INTO hdfs_canceled_write SELECT number FROM numbers(5)")
        assert_objects_count(started_cluster, 2)
        directories_before = hdfs_directories(fs)
        assert directories_before

        # Stream small blocks so that the write buffers (and with them the HDFS
        # files) are created before the exception cancels the insert.
        error = node.query_and_get_error(
            "INSERT INTO hdfs_canceled_write"
            " SELECT throwIf(number = 100000, 'canceled write') FROM numbers(200000)"
            " SETTINGS max_block_size = 4096, min_insert_block_size_rows = 4096"
        )
        assert "canceled write" in error

        # The canceled insert must roll back to the baseline without leaking
        # objects (the keys may differ - the sink rewrites `sizes.json`).
        assert_objects_count(started_cluster, 2)
        # The sink rewrites `sizes.json` upfront so it can roll back, which removes
        # the previous blob and empties its prefix directory inside this query; that
        # directory must survive the removal.
        assert hdfs_directories(fs) >= directories_before
        assert (
            node.query("SELECT * FROM hdfs_canceled_write ORDER BY id")
            == "0\n1\n2\n3\n4\n"
        )
    finally:
        node.query("DROP TABLE hdfs_canceled_write SYNC")
