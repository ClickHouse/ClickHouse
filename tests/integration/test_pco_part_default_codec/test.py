import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# A data part records its default compression codec in `default_compression_codec.txt`. That codec is
# fed raw — without a column type — into untyped streams such as the statistics and text-index
# serialization, and a mutation copies the source part default codec straight into the writer of the
# new part. Such a stream can only accept a codec that neither requires a column type (e.g. `PCO`) nor
# is experimental (e.g. the lossy `SZ3` / `ALP`, which reinterpret the opaque bytes as floating-point
# values). The table compression settings and the server `<compression>` selector already reject such
# codecs up front, but an attached or pre-fix part may still carry an unsuitable `CODEC(...)` in its
# metadata. The part-load path must enforce the same invariant (fall back to the server default codec)
# so the bad metadata cannot reach a mutation writer and fail with a confusing write-time error.
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_part_default_codec_is_sanitized_on_load(table, unsafe_codec):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (key UInt64, value Int64 STATISTICS(tdigest))
        ENGINE = MergeTree ORDER BY key
        """,
        settings={"allow_experimental_statistics": 1},
    )
    node.query(f"INSERT INTO {table} SELECT number, number FROM numbers(1000)")

    part = node.query(
        f"SELECT name FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()

    # Simulate an attached or pre-fix part whose recorded default codec is unsuitable for untyped streams.
    node.query(f"ALTER TABLE {table} DETACH PART '{part}'")
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo -n 'CODEC({unsafe_codec})' > {data_path}detached/{part}/default_compression_codec.txt",
        ]
    )
    # ATTACH PART assigns the re-attached part a fresh block number, so query by table (there is
    # exactly one active part) rather than by the pre-detach name.
    node.query(f"ALTER TABLE {table} ATTACH PART '{part}'")

    # The load path must have replaced the unsuitable codec with a usable default codec, so
    # `system.parts` never reports it as the part default.
    codec = node.query(
        f"SELECT default_compression_codec FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    assert codec and unsafe_codec not in codec, codec

    # A mutation reuses the source part default codec for untyped streams (here the `tdigest`
    # statistics of `value`); with the sanitized codec it must succeed instead of failing with a
    # write-time "cannot compress" error.
    node.query(
        f"ALTER TABLE {table} UPDATE value = value + 1 WHERE 1",
        settings={"mutations_sync": 2, "allow_experimental_statistics": 1},
    )
    assert node.query(f"SELECT count() FROM {table}").strip() == "1000"
    assert (
        node.query(f"SELECT sum(value) FROM {table}").strip()
        == str(sum(range(1000)) + 1000)
    )

    node.query(f"DROP TABLE {table} SYNC")


def test_part_default_codec_requiring_type_is_sanitized_on_load(start_cluster):
    # `PCO` reports `requiresColumnTypeToCompress()`: it cannot compress typeless data at all.
    check_part_default_codec_is_sanitized_on_load("t_pco_default", "PCO")


def test_part_default_codec_experimental_is_sanitized_on_load(start_cluster):
    # `ALP` is experimental but does NOT report `requiresColumnTypeToCompress()`, so the load-path guard
    # must reject experimental codecs too (not only type-requiring ones), matching the table-settings /
    # `<compression>` selector predicate. Without that, `ALP` would slip through and corrupt or reject
    # the opaque statistics bytes at the first mutation.
    check_part_default_codec_is_sanitized_on_load("t_alp_default", "ALP")
