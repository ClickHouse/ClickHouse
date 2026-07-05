"""Integration tests for the native Google Cloud Storage backend (google-cloud-cpp).

These exercise the native JSON-API client (selected by `use_native_gcs=1` for the table
function / `GCS` engine, and by `object_storage_type: gcs` for the storage disk) against a
`fake-gcs-server` emulator. minio cannot be used here because it only speaks the S3 API,
which is the *default* (S3-compatibility) path, not the native one.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node",
            main_configs=["configs/storage.xml"],
            with_gcs=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def gcs_url(path):
    # The ClickHouse server reaches the emulator via the docker-network hostname.
    return f"http://{cluster.gcs_host}:{cluster.gcs_port}/{cluster.gcs_bucket}/{path}"


def test_table_function_insert_select(started_cluster):
    node = started_cluster.instances["node"]
    url = gcs_url("tf/data.tsv")

    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64, b String') "
        f"SELECT number, toString(number) FROM numbers(100) "
        f"SETTINGS use_native_gcs = 1"
    )

    res = node.query(
        f"SELECT count(), sum(a) FROM gcs('{url}', NOSIGN, 'TSV', 'a UInt64, b String') "
        f"SETTINGS use_native_gcs = 1"
    )
    assert res.strip() == "100\t4950"


def test_table_function_glob(started_cluster):
    node = started_cluster.instances["node"]

    for i in range(3):
        node.query(
            f"INSERT INTO FUNCTION gcs('{gcs_url(f'glob/part{i}.tsv')}', NOSIGN, 'TSV', 'a UInt64') "
            f"SELECT number FROM numbers(10) "
            f"SETTINGS use_native_gcs = 1"
        )

    res = node.query(
        f"SELECT count() FROM gcs('{gcs_url('glob/*.tsv')}', NOSIGN, 'TSV', 'a UInt64') "
        f"SETTINGS use_native_gcs = 1"
    )
    assert res.strip() == "30"


def test_mergetree_on_gcs_disk(started_cluster):
    node = started_cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS gcs_mt SYNC")
    node.query(
        "CREATE TABLE gcs_mt (a UInt64, b String) ENGINE = MergeTree ORDER BY a "
        "SETTINGS storage_policy = 'gcs_policy'"
    )

    node.query("INSERT INTO gcs_mt SELECT number, toString(number) FROM numbers(1000)")
    node.query("INSERT INTO gcs_mt SELECT number, toString(number) FROM numbers(1000, 1000)")
    assert node.query("SELECT count() FROM gcs_mt").strip() == "2000"

    # Merge parts (writes new part blobs, deletes old ones) and re-read.
    node.query("OPTIMIZE TABLE gcs_mt FINAL")
    assert node.query("SELECT count() FROM gcs_mt").strip() == "2000"
    assert node.query("SELECT sum(a) FROM gcs_mt").strip() == str(sum(range(2000)))

    node.query("DROP TABLE gcs_mt SYNC")
