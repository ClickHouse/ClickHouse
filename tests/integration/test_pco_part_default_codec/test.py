import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# A data part records its default compression codec in `default_compression_codec.txt`. That codec is
# fed raw — without a column type — into untyped streams such as the statistics and text-index
# serialization, and a mutation copies the source part default codec straight into the writer of the
# new part. A codec that requires a column type (e.g. `PCO`) cannot compress there. The table
# compression settings and the server `<compression>` selector already reject such codecs up front,
# but an attached or pre-fix part may still carry a `CODEC(...)` that requires a column type in its
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


def test_part_default_codec_requiring_type_is_sanitized_on_load(start_cluster):
    node.query("DROP TABLE IF EXISTS t_pco_default SYNC")
    node.query(
        """
        CREATE TABLE t_pco_default (key UInt64, value Int64 STATISTICS(tdigest))
        ENGINE = MergeTree ORDER BY key
        """,
        settings={"allow_experimental_statistics": 1},
    )
    node.query("INSERT INTO t_pco_default SELECT number, number FROM numbers(1000)")

    part = node.query(
        "SELECT name FROM system.parts WHERE table = 't_pco_default' AND active"
    ).strip()
    data_path = node.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = 'default' AND name = 't_pco_default'"
    ).strip()

    # Simulate an attached or pre-fix part whose recorded default codec requires a column type.
    node.query(f"ALTER TABLE t_pco_default DETACH PART '{part}'")
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo -n 'CODEC(PCO)' > {data_path}detached/{part}/default_compression_codec.txt",
        ]
    )
    # ATTACH PART assigns the re-attached part a fresh block number, so query by table (there is
    # exactly one active part) rather than by the pre-detach name.
    node.query(f"ALTER TABLE t_pco_default ATTACH PART '{part}'")

    # The load path must have replaced the type-requiring codec with a usable default codec, so
    # `system.parts` never reports PCO as the part default.
    codec = node.query(
        "SELECT default_compression_codec FROM system.parts "
        "WHERE table = 't_pco_default' AND active"
    ).strip()
    assert codec and "PCO" not in codec, codec

    # A mutation reuses the source part default codec for untyped streams (here the `tdigest`
    # statistics of `value`); with the sanitized codec it must succeed instead of failing with
    # "Codec 'PCO' was created without a numeric column type and cannot compress".
    node.query(
        "ALTER TABLE t_pco_default UPDATE value = value + 1 WHERE 1",
        settings={"mutations_sync": 2, "allow_experimental_statistics": 1},
    )
    assert node.query("SELECT count() FROM t_pco_default").strip() == "1000"
    assert (
        node.query("SELECT sum(value) FROM t_pco_default").strip()
        == str(sum(range(1000)) + 1000)
    )

    node.query("DROP TABLE t_pco_default SYNC")
