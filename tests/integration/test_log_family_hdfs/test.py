import logging
import sys

import pytest
from helpers.cluster import ClickHouseCluster

from pyhdfs import HdfsClient


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

        fs = HdfsClient(hosts=cluster.hdfs_ip)
        fs.mkdirs("/clickhouse")

        yield cluster
    finally:
        cluster.shutdown()


def assert_objects_count(started_cluster, objects_count, path="data/"):
    fs = HdfsClient(hosts=started_cluster.hdfs_ip)
    hdfs_objects = fs.listdir("/clickhouse")
    assert objects_count == len(hdfs_objects)


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

    node.query("INSERT INTO hdfs_test SELECT number FROM numbers(5)")
    assert node.query("SELECT * FROM hdfs_test") == "0\n1\n2\n3\n4\n"
    assert_objects_count(started_cluster, files_overhead_per_insert + files_overhead)

    node.query("INSERT INTO hdfs_test SELECT number + 5 FROM numbers(3)")
    assert (
        node.query("SELECT * FROM hdfs_test order by id") == "0\n1\n2\n3\n4\n5\n6\n7\n"
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

    node.query("DROP TABLE hdfs_test")
