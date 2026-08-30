import os

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_metadata_compression_codec_change(started_cluster_iceberg_no_spark):
    """Every metadata file must be read with the codec that belongs to that file.

    A table may change `write.metadata.compression-codec` between metadata files, so the codec
    observed when the table object was first opened does not describe the files selected later.
    Reopening a `metadata.json` with the codec captured at open time made a table that started
    uncompressed and later wrote `<V>.gz.metadata.json` unreadable.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    table_name = "test_metadata_compression_codec_change_" + get_uuid_str()

    create_iceberg_table("local", instance, table_name, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {table_name} VALUES ('a', 1);")

    # Open the table, so the uncompressed codec of the current metadata file is captured.
    assert instance.query(f"SELECT x, y FROM {table_name} ORDER BY y").strip() == "a\t1"

    # The next metadata file is written with a different codec.
    instance.query(
        f"INSERT INTO {table_name} VALUES ('b', 2);",
        settings={"iceberg_metadata_compression_method": "gzip"},
    )

    assert instance.query(f"SELECT x, y FROM {table_name} ORDER BY y").strip() == "a\t1\nb\t2"
