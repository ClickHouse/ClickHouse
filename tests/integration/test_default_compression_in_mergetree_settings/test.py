import random
import string

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    main_configs=["configs/encryption_codec.xml", "configs/default_compression.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_compression_codec_byte(node, table_name, part_name, filename):
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='{table_name}'"
    ).strip()
    cmd = (
        "tail -c +17 {}/{}/{}.bin | od -x -N 1 | head -n 1 | awk '{{print $2}}'".format(
            data_path, part_name, filename
        )
    )
    return node.exec_in_container(["bash", "-c", cmd]).strip()


CODECS_MAPPING = {
    "NONE": "0002",
    "LZ4": "0082",
    "LZ4HC": "0082",  # not an error, same byte
    "ZSTD": "0090",
    "Multiple": "0091",
    "Delta": "0092",
    "T64": "0093",
    "AES_128_GCM_SIV": "0096",
}


def test_default_compression_codec_in_mergetree_settings(start_cluster):
    node.query("DROP TABLE IF EXISTS compression_table SYNC")
    node.query(
        """
    CREATE TABLE compression_table (
        key UInt64,
        column_ok Nullable(UInt64) CODEC(Delta, LZ4),
        column_array Array(Array(UInt64)) CODEC(T64, LZ4),
        column_default LowCardinality(Int64)
    ) ENGINE = ReplicatedMergeTree('/t', '0') ORDER BY tuple() PARTITION BY key
    SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, replace_long_file_name_to_hash = 0, default_compression_codec = 'ZSTD';
    """,
        settings={
            "allow_suspicious_codecs": "1",
            "allow_suspicious_low_cardinality_types": "1",
        },
    )

    node.query("INSERT INTO compression_table VALUES (1, 1, [[77]], 32)")

    assert (
        get_compression_codec_byte(node, "compression_table", "1_0_0_0", "column_ok")
        == CODECS_MAPPING["Multiple"]
    )
    assert (
        get_compression_codec_byte(
            node, "compression_table", "1_0_0_0", "column_ok.null"
        )
        == CODECS_MAPPING["LZ4"]
    )

    assert (
        get_compression_codec_byte(node, "compression_table", "1_0_0_0", "column_array")
        == CODECS_MAPPING["Multiple"]
    )
    assert (
        get_compression_codec_byte(
            node, "compression_table", "1_0_0_0", "column_array.size0"
        )
        == CODECS_MAPPING["LZ4"]
    )
    assert (
        get_compression_codec_byte(
            node, "compression_table", "1_0_0_0", "column_array.size1"
        )
        == CODECS_MAPPING["LZ4"]
    )

    assert (
        get_compression_codec_byte(
            node, "compression_table", "1_0_0_0", "column_default.dict"
        )
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        get_compression_codec_byte(
            node, "compression_table", "1_0_0_0", "column_default"
        )
        == CODECS_MAPPING["ZSTD"]
    )

    assert (
        get_compression_codec_byte(node, "compression_table", "1_0_0_0", "key")
        == CODECS_MAPPING["ZSTD"]
    )

    # Modify the default compression codec and check if newly added records use it
    node.query(
        "ALTER TABLE compression_table MODIFY SETTING default_compression_codec = 'AES_128_GCM_SIV'"
    )
    node.query("INSERT INTO compression_table VALUES (2, 1, [[77]], 32)")

    assert (
        get_compression_codec_byte(node, "compression_table", "1_0_0_0", "key")
        == CODECS_MAPPING["ZSTD"]
    )

    assert (
        get_compression_codec_byte(node, "compression_table", "2_0_0_0", "key")
        == CODECS_MAPPING["AES_128_GCM_SIV"]
    )
