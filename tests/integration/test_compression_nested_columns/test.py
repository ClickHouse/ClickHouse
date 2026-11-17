import random
import string

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


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
    cmd = f"tail -c +17 {data_path}/{part_name}/{filename}.bin | od -x -N 1 | head -n 1 | awk '{{print $2}}'"
    return node.exec_in_container(["bash", "-c", cmd]).strip()


CODECS_MAPPING = {
    "NONE": "0002",
    "LZ4": "0082",
    "LZ4HC": "0082",  # not an error, same byte
    "ZSTD": "0090",
    "Multiple": "0091",
    "Delta": "0092",
    "T64": "0093",
}


def test_nested_compression_codec(start_cluster):
    for i, node in enumerate([node1, node2]):
        node.query("DROP TABLE IF EXISTS compression_table SYNC")
        node.query(
            """
        CREATE TABLE compression_table (
            key UInt64,
            column_ok Nullable(UInt64) CODEC(Delta, LZ4),
            column_array Array(Array(UInt64)) CODEC(T64, LZ4),
            column_bad LowCardinality(Int64) CODEC(Delta)
        ) ENGINE = ReplicatedMergeTree('/t', '{}') ORDER BY tuple() PARTITION BY key
        SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, replace_long_file_name_to_hash = 0;
        """.format(
                i
            ),
            settings={
                "allow_suspicious_codecs": "1",
                "allow_suspicious_low_cardinality_types": "1",
            },
        )

    node1.query("INSERT INTO compression_table VALUES (1, 1, [[77]], 32)")

    node2.query("SYSTEM SYNC REPLICA compression_table", timeout=5)

    node1.query("DETACH TABLE compression_table")
    node2.query("DETACH TABLE compression_table")

    node1.query("ATTACH TABLE compression_table")
    node2.query("ATTACH TABLE compression_table")

    for node in [node1, node2]:
        assert (
            get_compression_codec_byte(
                node, "compression_table", "1_0_0_0", "column_ok"
            )
            == CODECS_MAPPING["Multiple"]
        )
        assert (
            get_compression_codec_byte(
                node, "compression_table", "1_0_0_0", "column_ok.null"
            )
            == CODECS_MAPPING["LZ4"]
        )

        assert (
            get_compression_codec_byte(
                node1, "compression_table", "1_0_0_0", "column_array"
            )
            == CODECS_MAPPING["Multiple"]
        )
        assert (
            get_compression_codec_byte(
                node2, "compression_table", "1_0_0_0", "column_array.size0"
            )
            == CODECS_MAPPING["LZ4"]
        )
        assert (
            get_compression_codec_byte(
                node2, "compression_table", "1_0_0_0", "column_array.size1"
            )
            == CODECS_MAPPING["LZ4"]
        )

        assert (
            get_compression_codec_byte(
                node2, "compression_table", "1_0_0_0", "column_bad.dict"
            )
            == CODECS_MAPPING["Delta"]
        )
        assert (
            get_compression_codec_byte(
                node1, "compression_table", "1_0_0_0", "column_bad"
            )
            == CODECS_MAPPING["NONE"]
        )

    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS compression_table SYNC")
