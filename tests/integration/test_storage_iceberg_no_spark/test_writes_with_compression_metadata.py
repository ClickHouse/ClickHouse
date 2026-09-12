import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str
)


def _metadata_dir(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_with_compression_metadata(started_cluster_iceberg_no_spark, format_version, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_writes_with_compression_metadata_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_no_spark, "(x String, y Int64)", format_version, use_version_hint=True, compression_method="gzip")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"iceberg_metadata_compression_method": "gzip"})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n'

    # gzip metadata must use the Iceberg spec extension `gz`, not the HTTP
    # Content-Encoding token `gzip`, otherwise Spark / Hadoop-catalog readers
    # cannot locate the metadata file (issue #109801).
    listing = instance.exec_in_container(
        ["bash", "-c", f"ls {_metadata_dir(TABLE_NAME)}"]
    )
    assert ".gz.metadata.json" in listing
    assert ".gzip.metadata.json" not in listing


@pytest.mark.parametrize("storage_type", ["local"])
def test_reads_legacy_gzip_metadata_extension(started_cluster_iceberg_no_spark, storage_type):
    # Tables written by older ClickHouse versions name gzip metadata
    # `v{N}.gzip.metadata.json` (the HTTP Content-Encoding token) rather than
    # the spec extension `v{N}.gz.metadata.json`. resolveMetadataFilenameFromVersionHint
    # must still locate such files through its legacy fallback, otherwise the
    # gz->gzip rename in issue #109801 would break reads of pre-existing tables.
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_reads_legacy_gzip_metadata_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_no_spark, "(x String, y Int64)", 2, use_version_hint=True, compression_method="gzip")
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"iceberg_metadata_compression_method": "gzip"})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n'

    metadata_dir = _metadata_dir(TABLE_NAME)

    # Rewrite the table on disk to the legacy `gzip` spelling and keep
    # version-hint.text a bare version number (as CH writes it), so the reopen
    # goes through resolveMetadataFilenameFromVersionHint's legacy branch.
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "set -e; "
            f"for f in {metadata_dir}/v*.gz.metadata.json; do "
            'mv "$f" "${f%.gz.metadata.json}.gzip.metadata.json"; done',
        ]
    )
    listing = instance.exec_in_container(["bash", "-c", f"ls {metadata_dir}"])
    assert ".gzip.metadata.json" in listing
    assert ".gz.metadata.json" not in listing
    # version-hint.text must stay a bare number to exercise the resolver branch.
    version_hint = instance.exec_in_container(
        ["bash", "-c", f"cat {metadata_dir}/version-hint.text"]
    ).strip()
    assert version_hint.isdigit()

    # Re-read the table through a fresh table function (which resolves the
    # metadata from scratch via the version hint, exercising the legacy branch
    # of resolveMetadataFilenameFromVersionHint) and confirm ClickHouse still
    # reads the legacy `gzip`-spelled metadata files.
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    reopen_query = (
        f"SELECT * FROM icebergLocal(local, path = '{table_path}', "
        f"SETTINGS iceberg_use_version_hint = 1) ORDER BY ALL"
    )
    assert instance.query(
        reopen_query,
        settings={"iceberg_metadata_compression_method": "gzip"},
    ) == '123\t1\n'


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_after_metadata_compression_switch(started_cluster_iceberg_no_spark, storage_type):
    # Regression test: `getLatestOrExplicitMetadataFileAndVersion` resolves the
    # compression of the *current* metadata file from its name (e.g. `.gz.metadata.json`),
    # but `getPlan` (full-table `OPTIMIZE`, not `OPTIMIZE ... MANIFEST`) used to discard
    # that resolved codec and re-read the file with the write-target codec from the
    # current query's settings instead. If the table's on-disk metadata was written
    # with a different `iceberg_metadata_compression_method` than the `OPTIMIZE` query
    # uses, the stale codec fails to decompress the file.
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_after_compression_switch_" + storage_type + "_" + get_uuid_str()

    # Create the table with gzip metadata, so `v1.gz.metadata.json` is on disk from the
    # start. The write codec for subsequent metadata files is inherited from the current
    # metadata file (`IcebergWrites.cpp` takes it from
    # `getLatestMetadataFileAndVersionWithCatalog`, not from the writing query's
    # `iceberg_metadata_compression_method`), so creating the table uncompressed here
    # would keep every later version uncompressed too and the `OPTIMIZE` below would
    # never exercise the codec mismatch this test is about.
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(x String, y Int64)",
        2,
        compression_method="gzip",
    )

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('a', 1);")
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('b', 2);")
    listing = instance.exec_in_container(["bash", "-c", f"ls {_metadata_dir(TABLE_NAME)}"])
    assert ".gz.metadata.json" in listing

    # OPTIMIZE with the default (uncompressed) metadata codec must still be able to
    # read the gzip-compressed metadata file that is actually on disk.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == 'a\t1\nb\t2\n'
