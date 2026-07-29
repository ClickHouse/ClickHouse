"""Integration tests for the native Google Cloud Storage backend (google-cloud-cpp).

These exercise the native JSON-API client (selected by `use_native_gcs=1` for the table
function / `GCS` engine, and by `object_storage_type: gcs` for the storage disk) against a
`fake-gcs-server` emulator. minio cannot be used here because it only speaks the S3 API,
which is the *default* (S3-compatibility) path, not the native one.

The native backend requires ClickHouse to be built with the google-cloud-cpp SDK
(`USE_GOOGLE_CLOUD=1`, the default on Linux amd64/aarch64). The whole module is skipped on
builds without it. The disk is defined inline in the CREATE query rather than in a static
config so that the server starts even on such builds (a static `gcs` disk would otherwise
fail startup with UNKNOWN_ELEMENT_IN_CONFIG).
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance("node", with_gcs=True)
        cluster.start()

        node = cluster.instances["node"]
        built_with_sdk = (
            node.query(
                "SELECT value FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'"
            ).strip()
            == "1"
        )
        if not built_with_sdk:
            pytest.skip("ClickHouse was built without the google-cloud-cpp SDK (USE_GOOGLE_CLOUD=0)")

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
    disk_endpoint = f"http://{cluster.gcs_host}:{cluster.gcs_port}/{cluster.gcs_bucket}/mergetree/"

    node.query("DROP TABLE IF EXISTS gcs_mt SYNC")
    node.query(
        "CREATE TABLE gcs_mt (a UInt64, b String) ENGINE = MergeTree ORDER BY a "
        "SETTINGS disk = disk("
        "  name = 'gcs_disk_test',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{disk_endpoint}',"
        "  no_sign_request = true"
        ")"
    )

    node.query("INSERT INTO gcs_mt SELECT number, toString(number) FROM numbers(1000)")
    node.query("INSERT INTO gcs_mt SELECT number, toString(number) FROM numbers(1000, 1000)")
    assert node.query("SELECT count() FROM gcs_mt").strip() == "2000"

    # Merge parts (writes new part blobs, deletes old ones) and re-read.
    node.query("OPTIMIZE TABLE gcs_mt FINAL")
    assert node.query("SELECT count() FROM gcs_mt").strip() == "2000"
    assert node.query("SELECT sum(a) FROM gcs_mt").strip() == str(sum(range(2000)))

    node.query("DROP TABLE gcs_mt SYNC")


def test_schema_inference_cache(started_cluster):
    """The native `gcs` schema cache must be visible in `system.schema_inference_cache` and
    clearable with `SYSTEM DROP SCHEMA CACHE FOR GCS` (otherwise a stale inferred schema
    could only be purged by restarting the server)."""
    node = started_cluster.instances["node"]
    url = gcs_url("schema_cache/data.tsv")

    node.query("SYSTEM DROP SCHEMA CACHE FOR GCS")
    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64, b String') "
        f"SELECT number, toString(number) FROM numbers(10) "
        f"SETTINGS use_native_gcs = 1"
    )

    # Reading without an explicit structure infers the schema and caches it.
    node.query(f"SELECT count() FROM gcs('{url}', NOSIGN, 'TSV') SETTINGS use_native_gcs = 1")

    assert (
        node.query(
            "SELECT count() FROM system.schema_inference_cache "
            f"WHERE storage = 'GCS' AND source LIKE '%schema_cache/data.tsv'"
        ).strip()
        == "1"
    )

    node.query("SYSTEM DROP SCHEMA CACHE FOR GCS")
    assert (
        node.query(
            "SELECT count() FROM system.schema_inference_cache WHERE storage = 'GCS'"
        ).strip()
        == "0"
    )
