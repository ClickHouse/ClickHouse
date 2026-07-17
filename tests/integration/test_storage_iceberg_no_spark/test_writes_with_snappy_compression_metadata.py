import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
@pytest.mark.parametrize("snappy_mode", ["basic", "framed"])
def test_writes_with_snappy_compression_metadata(
    started_cluster_iceberg_no_spark, format_version, storage_type, snappy_mode
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_with_snappy_compression_metadata_"
        + storage_type
        + "_"
        + snappy_mode
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(x String, y Int64)",
        format_version,
        use_version_hint=True,
        compression_method="snappy",
    )

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ""

    # Iceberg metadata snappy is always the Hadoop block format, independent of `snappy_mode`.
    # The wire format is not encoded in the `.snappy.metadata.json` suffix, so the metadata must
    # round-trip regardless of the session value of `snappy_mode`. Before this was pinned, writing
    # with `snappy_mode = 'framed'` produced framed metadata that the reader (which always decodes
    # the basic Hadoop block format) could not read back.
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);",
        settings={
            "iceberg_metadata_compression_method": "snappy",
            "snappy_mode": snappy_mode,
        },
    )
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == "123\t1\n"
