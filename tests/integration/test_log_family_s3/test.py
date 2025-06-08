import logging
import sys

import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/storage_configuration.xml", "configs/ssl.xml"],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def assert_objects_count(cluster, objects_count, path="data/"):
    minio = cluster.minio_client
    s3_objects = list(minio.list_objects(cluster.minio_bucket, path, recursive=True))
    if objects_count != len(s3_objects):
        for s3_object in s3_objects:
            object_meta = minio.stat_object(cluster.minio_bucket, s3_object.object_name)
            logging.info("Existing S3 object: %s", str(object_meta))
        assert objects_count == len(s3_objects)


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
    [
        pytest.param("TinyLog", 1, 1, id="TinyLog"),
        pytest.param("Log", 1, 2, id="Log"),
        pytest.param("StripeLog", 1, 2, id="StripeLog"),
    ],
)
def test_log_family_s3(cluster, log_engine, files_overhead, files_overhead_per_insert):
    node = cluster.instances["node"]

    node.query(
        "CREATE TABLE s3_test (id UInt64) ENGINE={} SETTINGS disk = 's3'".format(
            log_engine
        )
    )

    try:
        node.query("INSERT INTO s3_test SELECT number FROM numbers(5)")
        assert node.query("SELECT * FROM s3_test") == "0\n1\n2\n3\n4\n"
        assert_objects_count(cluster, files_overhead_per_insert + files_overhead)

        node.query("INSERT INTO s3_test SELECT number + 5 FROM numbers(3)")
        assert (
            node.query("SELECT * FROM s3_test order by id")
            == "0\n1\n2\n3\n4\n5\n6\n7\n"
        )
        assert_objects_count(cluster, files_overhead_per_insert * 2 + files_overhead)

        node.query("INSERT INTO s3_test SELECT number + 8 FROM numbers(1)")
        assert (
            node.query("SELECT * FROM s3_test order by id")
            == "0\n1\n2\n3\n4\n5\n6\n7\n8\n"
        )
        assert_objects_count(cluster, files_overhead_per_insert * 3 + files_overhead)

        node.query("TRUNCATE TABLE s3_test")
        assert_objects_count(cluster, 0)
    finally:
        node.query("DROP TABLE s3_test")


# Imitate case when error occurs while inserting into table.
# For examle S3::TooManyRequests.
# In that case we can update data file, but not the size file.
# So due to exception we should do truncate of the data file to undo the insert query.
# See FileChecker::repair().
def test_stripe_log_truncate(cluster):
    node = cluster.instances["node"]

    node.query(
        """
        CREATE TABLE stripe_table (
            a int
        ) ENGINE = StripeLog()
        SETTINGS storage_policy='s3_no_retries'
        """
    )

    node.query("SYSTEM ENABLE FAILPOINT stripe_log_sink_write_fallpoint")
    node.query(
        """
        INSERT INTO stripe_table SELECT number FROM numbers(10)
        """,
        ignore_error=True,
    )
    node.query("SYSTEM DISABLE FAILPOINT stripe_log_sink_write_fallpoint")
    node.query("SELECT count(*) FROM stripe_table") == "0\n"
    node.query("INSERT INTO stripe_table SELECT number FROM numbers(10)")
    node.query("SELECT count(*) FROM stripe_table") == "10\n"

    # Make sure that everything is okey with the table after restart.
    node.query("DETACH TABLE stripe_table")
    node.query("ATTACH TABLE stripe_table")

    assert node.query("DROP TABLE stripe_table") == ""
