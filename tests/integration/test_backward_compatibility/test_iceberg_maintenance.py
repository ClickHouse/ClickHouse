import io
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.test_tools import assert_eq_with_retry

# `ALTER TABLE ... EXECUTE expire_snapshots` appeared in 26.3, so an older baseline could not
# maintain the table at all. The tag is a moving minor tag: the old side follows 26.3 patch releases.
OLD_VERSION = "26.3"

cluster = ClickHouseCluster(__file__)
old_node = cluster.add_instance(
    "old_node",
    image="clickhouse/clickhouse-server",
    tag=OLD_VERSION,
    with_installed_binary=True,
    with_minio=True,
)
new_node = cluster.add_instance("new_node", with_minio=True)

TABLE_PATH = "iceberg_maintenance"
ENGINE = f"IcebergS3('http://minio1:9001/root/{TABLE_PATH}/', 'minio', '{minio_secret_key}')"

WRITE_SETTINGS = {"allow_insert_into_iceberg": 1, "mutations_sync": 2}
EXPIRE_SETTINGS = {"allow_insert_into_iceberg": 1, "allow_experimental_expire_snapshots": 1}
ORPHAN_SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "allow_iceberg_remove_orphan_files": 1,
    "iceberg_orphan_files_min_retention_seconds": 0,
}

SELECT_ALL = "SELECT id, v FROM t ORDER BY id FORMAT TSV"
SNAPSHOT_COUNT = "SELECT count() FROM system.iceberg_history WHERE database = 'default' AND table = 't'"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def expire_snapshots(node):
    node.query(
        "ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 2)",
        settings=EXPIRE_SETTINGS,
    )
    assert node.query(SNAPSHOT_COUNT) == "2\n"


def test_iceberg_table_maintained_across_versions(started_cluster):
    old_node.query(
        f"CREATE TABLE t (id Int64, v String) ENGINE = {ENGINE} SETTINGS iceberg_format_version = 2",
        settings=WRITE_SETTINGS,
    )
    for values in ["(1, 'a'), (2, 'b')", "(3, 'c')", "(4, 'd')"]:
        old_node.query(f"INSERT INTO t VALUES {values}", settings=WRITE_SETTINGS)
    # Writes a position delete file.
    old_node.query("ALTER TABLE t DELETE WHERE id = 2", settings=WRITE_SETTINGS)
    expire_snapshots(old_node)

    expected = "1\ta\n3\tc\n4\td\n"
    assert old_node.query(SELECT_ALL) == expected

    new_node.query(f"CREATE TABLE t ENGINE = {ENGINE}")
    assert new_node.query(SELECT_ALL) == expected
    assert new_node.query(SNAPSHOT_COUNT) == "2\n"

    new_node.query("INSERT INTO t VALUES (5, 'e')", settings=WRITE_SETTINGS)
    expire_snapshots(new_node)

    orphan = f"{TABLE_PATH}/data/orphan-{uuid.uuid4().hex}.parquet"
    cluster.minio_client.put_object(cluster.minio_bucket, orphan, io.BytesIO(b"orphan"), 6)
    # Object timestamps have one-second precision and come from MinIO's clock.
    cutoff = new_node.query("SELECT now() + 2").strip()
    assert_eq_with_retry(new_node, f"SELECT now() > toDateTime('{cutoff}')", "1")

    result = new_node.query(
        f"ALTER TABLE t EXECUTE remove_orphan_files(older_than = '{cutoff}')",
        settings=ORPHAN_SETTINGS,
    )
    rows = (line.split("\t") for line in result.strip().splitlines())
    removed = {name: value for name, value in rows if value != "0"}
    # Metadata files are not checked: https://github.com/ClickHouse/ClickHouse/issues/118812
    removed.pop("deleted_metadata_files_count", None)
    # Only the planted file goes: nothing written by the old version is taken for an orphan.
    assert removed == {"deleted_data_files_count": "1"}

    expected += "5\te\n"
    assert new_node.query(SELECT_ALL) == expected

    # A replica still on the old version reads the table maintained by the new one. It does not write:
    # 26.3 copies manifest-list entries of newer writers without converting them and corrupts the
    # table: https://github.com/ClickHouse/ClickHouse/issues/125399
    assert old_node.query(SELECT_ALL) == expected
