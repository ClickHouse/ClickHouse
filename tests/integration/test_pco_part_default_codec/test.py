import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# A data part records its default compression codec in `default_compression_codec.txt`. That codec is
# fed raw — without a column type — into untyped streams such as the statistics and text-index
# serialization, and a mutation copies the source part default codec straight into the writer of the
# new part. Such a stream can only accept a codec that does not require a column type (`PCO` cannot
# compress typeless data at all, and `ALP` reinterprets the opaque bytes as floating-point values and
# rejects a blob whose size is not a multiple of the element width). The table compression settings
# and the server `<compression>` selector already reject such codecs up front, but an attached or
# pre-fix part may still carry an unsuitable `CODEC(...)` in its metadata. The part-load path must
# enforce the same invariant (fall back to the server default codec) so the bad metadata cannot reach
# a mutation writer and fail with a confusing write-time error.
node = cluster.add_instance("node")
# A node whose default codec comes from a size-based `<compression>` selector (parts >= 1000 bytes use
# `ZSTD(3)`, smaller parts fall through to `LZ4`) rather than from the `default_compression_codec`
# setting, to exercise the selector's size thresholds when sanitizing a part default codec on load.
node_selector = cluster.add_instance(
    "node_selector", main_configs=["configs/compression_selector.xml"]
)
# A node whose `<compression>` selector depends on `min_part_size_ratio` (parts making up at least
# half of the table's active size use `ZSTD(3)`, else `LZ4`), to exercise the ratio computation when
# sanitizing a part default codec on load: the part is not counted as active yet at that point, so its
# size must be added back to the active total for the ratio to match an ordinary write.
node_ratio_selector = cluster.add_instance(
    "node_ratio_selector", main_configs=["configs/compression_ratio_selector.xml"]
)


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


def test_part_default_codec_alp_untyped_is_sanitized_on_load(start_cluster):
    # `ALP` built without a column type falls back to the `Float64` element width and reports
    # `requiresColumnTypeToCompress()`: it reinterprets the bytes as floating-point values and rejects
    # any blob whose size is not a multiple of that width. Without the guard it would slip through and
    # reject the opaque statistics bytes at the first mutation.
    check_part_default_codec_is_sanitized_on_load("t_alp_default", "ALP")


def test_sanitized_part_default_codec_follows_selector_part_size(start_cluster):
    # Sanitizing the part default must evaluate the server `<compression>` selector with the part's real
    # size, not a zero size. Otherwise a large attached / pre-fix part whose recorded default is `PCO`
    # would sanitize to the selector's smallest-part case (`LZ4`) even though ordinary writes for a part
    # of this size choose `ZSTD(3)` — and the column-rewrite mutation path reuses that cached part
    # default codec for every rewritten column, silently moving the data off `ZSTD(3)`.
    table = "t_selector_default"
    node_selector.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node_selector.query(
        f"""
        CREATE TABLE {table} (key UInt64, value Int64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    node_selector.query(f"INSERT INTO {table} SELECT number, number FROM numbers(1000)")

    part = node_selector.query(
        f"SELECT name FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    data_path = node_selector.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()

    node_selector.query(f"ALTER TABLE {table} DETACH PART '{part}'")
    node_selector.exec_in_container(
        [
            "bash",
            "-c",
            f"echo -n 'CODEC(PCO)' > {data_path}detached/{part}/default_compression_codec.txt",
        ]
    )
    node_selector.query(f"ALTER TABLE {table} ATTACH PART '{part}'")

    # The part is well above the selector's `min_part_size = 1000`, so the sanitized default must be
    # `ZSTD(3)` (the case this size warrants), not the smallest-part `LZ4` a zero size would select.
    codec = node_selector.query(
        f"SELECT default_compression_codec FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    assert "ZSTD" in codec and "LZ4" not in codec, codec

    # The column-rewrite mutation reuses the (now size-appropriate) source part default codec, so the
    # mutated part keeps `ZSTD(3)` rather than drifting to `LZ4`.
    node_selector.query(
        f"ALTER TABLE {table} UPDATE value = value + 1 WHERE 1",
        settings={"mutations_sync": 2},
    )
    codec = node_selector.query(
        f"SELECT default_compression_codec FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    assert "ZSTD" in codec and "LZ4" not in codec, codec
    assert node_selector.query(f"SELECT count() FROM {table}").strip() == "1000"

    node_selector.query(f"DROP TABLE {table} SYNC")


def test_sanitized_part_default_codec_follows_selector_part_size_ratio(start_cluster):
    # Sanitizing the part default must evaluate a `min_part_size_ratio` selector case as if the part
    # were already active. `loadDefaultCompressionCodec` runs before the part is committed as active,
    # so `getTotalActiveSizeInBytes` does not include it yet: for a lone attached part the naive ratio
    # is `0` (matching only the smallest-ratio case, `LZ4`), while an ordinary write of the same part —
    # e.g. the full-rewrite mutation path — computes ratio `1` once the part is active and chooses
    # `ZSTD(3)`. The part's size must therefore be added back to the active total for the ratio.
    table = "t_ratio_selector_default"
    node_ratio_selector.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node_ratio_selector.query(
        f"""
        CREATE TABLE {table} (key UInt64, value Int64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    node_ratio_selector.query(
        f"INSERT INTO {table} SELECT number, number FROM numbers(1000)"
    )

    part = node_ratio_selector.query(
        f"SELECT name FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    data_path = node_ratio_selector.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = 'default' AND name = '{table}'"
    ).strip()

    # After the detach the table has no active parts, so on the re-attach the part makes up the whole
    # table: ratio `1` under the fixed computation, ratio `0` under the naive one.
    node_ratio_selector.query(f"ALTER TABLE {table} DETACH PART '{part}'")
    node_ratio_selector.exec_in_container(
        [
            "bash",
            "-c",
            f"echo -n 'CODEC(PCO)' > {data_path}detached/{part}/default_compression_codec.txt",
        ]
    )
    node_ratio_selector.query(f"ALTER TABLE {table} ATTACH PART '{part}'")

    # The lone part is the entire active size (ratio `1` >= `min_part_size_ratio` `0.5`), so the
    # sanitized default must be `ZSTD(3)`, not the small-ratio `LZ4` the not-yet-active ratio `0`
    # would select.
    codec = node_ratio_selector.query(
        f"SELECT default_compression_codec FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    assert "ZSTD" in codec and "LZ4" not in codec, codec

    # The column-rewrite mutation reuses the cached source part default codec directly, so the mutated
    # part must also keep `ZSTD(3)` — this is the path a wrongly-cached small-ratio codec would poison.
    node_ratio_selector.query(
        f"ALTER TABLE {table} UPDATE value = value + 1 WHERE 1",
        settings={"mutations_sync": 2},
    )
    codec = node_ratio_selector.query(
        f"SELECT default_compression_codec FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    assert "ZSTD" in codec and "LZ4" not in codec, codec
    assert node_ratio_selector.query(f"SELECT count() FROM {table}").strip() == "1000"

    node_ratio_selector.query(f"DROP TABLE {table} SYNC")
