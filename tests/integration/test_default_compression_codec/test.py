import random
import string

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/default_compression.xml",
        "configs/wide_parts_only.xml",
        "configs/long_names.xml",
    ],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/default_compression.xml",
        "configs/wide_parts_only.xml",
        "configs/long_names.xml",
    ],
    with_zookeeper=True,
)
node4 = cluster.add_instance("node4")

node5 = cluster.add_instance(
    "node5",
    main_configs=[
        "configs/force_zstd3.xml",
    ],
)
node6 = cluster.add_instance(
    "node6",
    main_configs=["configs/force_zstd3.xml"],
    user_configs=["configs/small_projection_rebuild_blocks.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_compression_codec_byte(node, table_name, part_name):
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='{table_name}'"
    ).strip()
    cmd = f"tail -c +17 {data_path}/{part_name}/data1.bin | od -x -N 1 | head -n 1 | awk '{{print $2}}'"
    return node.exec_in_container(["bash", "-c", cmd]).strip()


def get_second_multiple_codec_byte(node, table_name, part_name):
    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='{table_name}'"
    ).strip()
    cmd = f"tail -c +17 {data_path}/{part_name}/data1.bin | od -x -j 11 -N 1 | head -n 1 | awk '{{print $2}}'"
    return node.exec_in_container(["bash", "-c", cmd]).strip()


def get_random_string(length):
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


CODECS_MAPPING = {
    "LZ4": "0082",
    "LZ4HC": "0082",  # not an error, same byte
    "ZSTD": "0090",
    "Multiple": "0091",
}


def test_default_codec_single(start_cluster):
    for i, node in enumerate([node1, node2]):
        node.query(
            """
        CREATE TABLE compression_table (
            key UInt64,
            data1 String CODEC(Default)
        ) ENGINE = ReplicatedMergeTree('/t', '{}') ORDER BY tuple() PARTITION BY key;
        """.format(
                i
            )
        )

    # ZSTD(10) and ZSTD(10) after merge
    node1.query("INSERT INTO compression_table VALUES (1, 'x')")

    # ZSTD(10) and LZ4HC(10) after merge
    node1.query(
        "INSERT INTO compression_table VALUES (2, '{}')".format(get_random_string(2048))
    )

    # ZSTD(10) and LZ4 after merge
    node1.query(
        "INSERT INTO compression_table VALUES (3, '{}')".format(
            get_random_string(22048)
        )
    )

    node2.query("SYSTEM SYNC REPLICA compression_table", timeout=15)

    # to reload parts
    node1.query("DETACH TABLE compression_table")
    node2.query("DETACH TABLE compression_table")

    node1.query("ATTACH TABLE compression_table")
    node2.query("ATTACH TABLE compression_table")

    node1.query("SYSTEM FLUSH LOGS")
    node2.query("SYSTEM FLUSH LOGS")

    # Same codec for all
    assert (
        get_compression_codec_byte(node1, "compression_table", "1_0_0_0")
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '1_0_0_0'"
        )
        == "ZSTD(10)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '1_0_0_0'"
        )
        == "ZSTD(10)\n"
    )

    assert (
        get_compression_codec_byte(node1, "compression_table", "2_0_0_0")
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '2_0_0_0'"
        )
        == "ZSTD(10)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '2_0_0_0'"
        )
        == "ZSTD(10)\n"
    )

    assert (
        get_compression_codec_byte(node1, "compression_table", "3_0_0_0")
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '3_0_0_0'"
        )
        == "ZSTD(10)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '3_0_0_0'"
        )
        == "ZSTD(10)\n"
    )

    # just to be sure that replication works
    node1.query("OPTIMIZE TABLE compression_table FINAL")

    node2.query("SYSTEM SYNC REPLICA compression_table", timeout=15)

    # to reload parts
    node1.query("DETACH TABLE compression_table")
    node2.query("DETACH TABLE compression_table")

    node1.query("ATTACH TABLE compression_table")
    node2.query("ATTACH TABLE compression_table")

    node1.query("SYSTEM FLUSH LOGS")
    node2.query("SYSTEM FLUSH LOGS")

    assert (
        get_compression_codec_byte(node1, "compression_table", "1_0_0_1")
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '1_0_0_1'"
        )
        == "ZSTD(10)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '1_0_0_1'"
        )
        == "ZSTD(10)\n"
    )

    assert (
        get_compression_codec_byte(node1, "compression_table", "2_0_0_1")
        == CODECS_MAPPING["LZ4HC"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '2_0_0_1'"
        )
        == "LZ4HC(5)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '2_0_0_1'"
        )
        == "LZ4HC(5)\n"
    )

    assert (
        get_compression_codec_byte(node1, "compression_table", "3_0_0_1")
        == CODECS_MAPPING["LZ4"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '3_0_0_1'"
        )
        == "LZ4\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table' and name = '3_0_0_1'"
        )
        == "LZ4\n"
    )

    assert node1.query("SELECT COUNT() FROM compression_table") == "3\n"
    assert node2.query("SELECT COUNT() FROM compression_table") == "3\n"

    node1.query("DROP TABLE compression_table SYNC")
    node2.query("DROP TABLE compression_table SYNC")


def test_default_codec_multiple(start_cluster):
    for i, node in enumerate([node1, node2]):
        node.query(
            """
        CREATE TABLE compression_table_multiple (
            key UInt64,
            data1 String CODEC(NONE, Default)
        ) ENGINE = ReplicatedMergeTree('/d', '{}') ORDER BY tuple() PARTITION BY key;
        """.format(
                i
            ),
            settings={"allow_suspicious_codecs": 1},
        )

    # ZSTD(10) and ZSTD(10) after merge
    node1.query("INSERT INTO compression_table_multiple VALUES (1, 'x')")

    # ZSTD(10) and LZ4HC(10) after merge
    node1.query(
        "INSERT INTO compression_table_multiple VALUES (2, '{}')".format(
            get_random_string(2048)
        )
    )

    # ZSTD(10) and LZ4 after merge
    node1.query(
        "INSERT INTO compression_table_multiple VALUES (3, '{}')".format(
            get_random_string(22048)
        )
    )

    node2.query("SYSTEM SYNC REPLICA compression_table_multiple", timeout=15)

    # Same codec for all
    assert (
        get_compression_codec_byte(node1, "compression_table_multiple", "1_0_0_0")
        == CODECS_MAPPING["Multiple"]
    )
    assert (
        get_second_multiple_codec_byte(node1, "compression_table_multiple", "1_0_0_0")
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '1_0_0_0'"
        )
        == "ZSTD(10)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '1_0_0_0'"
        )
        == "ZSTD(10)\n"
    )

    assert (
        get_compression_codec_byte(node1, "compression_table_multiple", "2_0_0_0")
        == CODECS_MAPPING["Multiple"]
    )
    assert (
        get_second_multiple_codec_byte(node1, "compression_table_multiple", "2_0_0_0")
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '2_0_0_0'"
        )
        == "ZSTD(10)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '2_0_0_0'"
        )
        == "ZSTD(10)\n"
    )

    assert (
        get_compression_codec_byte(node1, "compression_table_multiple", "3_0_0_0")
        == CODECS_MAPPING["Multiple"]
    )
    assert (
        get_second_multiple_codec_byte(node1, "compression_table_multiple", "3_0_0_0")
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '3_0_0_0'"
        )
        == "ZSTD(10)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '3_0_0_0'"
        )
        == "ZSTD(10)\n"
    )

    node2.query("SYSTEM SYNC REPLICA compression_table_multiple", timeout=15)

    node1.query("OPTIMIZE TABLE compression_table_multiple FINAL")

    node2.query("SYSTEM SYNC REPLICA compression_table_multiple", timeout=15)

    assert (
        get_compression_codec_byte(node1, "compression_table_multiple", "1_0_0_1")
        == CODECS_MAPPING["Multiple"]
    )
    assert (
        get_second_multiple_codec_byte(node1, "compression_table_multiple", "1_0_0_1")
        == CODECS_MAPPING["ZSTD"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '1_0_0_1'"
        )
        == "ZSTD(10)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '1_0_0_1'"
        )
        == "ZSTD(10)\n"
    )

    assert (
        get_compression_codec_byte(node1, "compression_table_multiple", "2_0_0_1")
        == CODECS_MAPPING["Multiple"]
    )
    assert (
        get_second_multiple_codec_byte(node1, "compression_table_multiple", "2_0_0_1")
        == CODECS_MAPPING["LZ4HC"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '2_0_0_1'"
        )
        == "LZ4HC(5)\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '2_0_0_1'"
        )
        == "LZ4HC(5)\n"
    )

    assert (
        get_compression_codec_byte(node1, "compression_table_multiple", "3_0_0_1")
        == CODECS_MAPPING["Multiple"]
    )
    assert (
        get_second_multiple_codec_byte(node1, "compression_table_multiple", "3_0_0_1")
        == CODECS_MAPPING["LZ4"]
    )
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '3_0_0_1'"
        )
        == "LZ4\n"
    )
    assert (
        node2.query(
            "SELECT default_compression_codec FROM system.parts WHERE table = 'compression_table_multiple' and name = '3_0_0_1'"
        )
        == "LZ4\n"
    )

    assert node1.query("SELECT COUNT() FROM compression_table_multiple") == "3\n"
    assert node2.query("SELECT COUNT() FROM compression_table_multiple") == "3\n"

    node1.query("DROP TABLE compression_table_multiple SYNC")
    node2.query("DROP TABLE compression_table_multiple SYNC")


def test_default_codec_for_compact_parts(start_cluster):
    node4.query(
        """
    CREATE TABLE compact_parts_table (
        key UInt64,
        data String
    )
    ENGINE MergeTree ORDER BY tuple()
    """
    )

    node4.query("INSERT INTO compact_parts_table VALUES (1, 'Hello world')")
    assert node4.query("SELECT COUNT() FROM compact_parts_table") == "1\n"

    node4.query("ALTER TABLE compact_parts_table DETACH PART 'all_1_1_0'")

    data_path = node4.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='compact_parts_table'"
    ).strip()
    node4.exec_in_container(
        ["rm", f"{data_path}detached/all_1_1_0/default_compression_codec.txt"]
    )

    node4.query("ALTER TABLE compact_parts_table ATTACH PART 'all_1_1_0'")

    assert node4.query("SELECT COUNT() FROM compact_parts_table") == "1\n"

    node4.query("DETACH TABLE compact_parts_table")
    node4.query("ATTACH TABLE compact_parts_table")

    assert node4.query("SELECT COUNT() FROM compact_parts_table") == "1\n"
    node4.query("DROP TABLE compact_parts_table SYNC")


def test_missing_codec_file_fails_closed_for_modern_part(start_cluster):
    # A part can be missing `default_compression_codec.txt` because it is genuinely legacy (that file
    # was introduced long ago) or because a modern part lost it, for example during
    # detach/copy/restore. When every column has an explicit CODEC, no column proves the default
    # codec, so `IMergeTreeDataPart::detectDefaultCompressionCodec` cannot read it from a column
    # `.bin` and cannot recover it. `checksums.txt` records the built-in codec rather than the part
    # default, so a modern part that lost its mandatory codec file must fail closed.
    # `min_bytes_for_wide_part = 0` keeps the part `Wide`, so the mutation below rewrites a single
    # column and hardlinks the rest. A `Compact` part would take the full-rewrite path, whose codec
    # is derived from the current metadata and is therefore exact, and the provenance marker
    # asserted below would not be involved at all.
    node4.query(
        """
    CREATE TABLE no_codec_file (
        key UInt64 CODEC(ZSTD(1)),
        data String CODEC(ZSTD(1))
    )
    ENGINE MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0
    """
    )

    # Two inserts and a merge, so the part whose codec file we drop is a merged part.
    node4.query("INSERT INTO no_codec_file VALUES (1, 'Hello world')")
    node4.query("INSERT INTO no_codec_file VALUES (2, 'Goodbye world')")
    node4.query("OPTIMIZE TABLE no_codec_file FINAL")

    part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='no_codec_file' AND active"
    ).strip()

    node4.query(f"ALTER TABLE no_codec_file DETACH PART '{part_name}'")

    data_path = node4.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='no_codec_file'"
    ).strip()
    node4.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )

    with pytest.raises(Exception, match="Cannot recover the default compression codec"):
        node4.query(f"ALTER TABLE no_codec_file ATTACH PART '{part_name}'")

    # A mutation descendant of an old all-explicit-codec part has no data column that can recover
    # the old default. `UNKNOWN` is the durable provenance marker emitted for that state. It must
    # keep the descendant attachable even though its new `checksums.txt` is a modern ZSTD frame.
    node4.exec_in_container(
        [
            "python3",
            "-c",
            "import sys; open(sys.argv[1], 'w').write('UNKNOWN')",
            f"{data_path}detached/{part_name}/default_compression_codec.txt",
        ]
    )
    node4.query(f"ALTER TABLE no_codec_file ATTACH PART '{part_name}'")
    node4.query(
        "ALTER TABLE no_codec_file UPDATE data = concat(data, '!') WHERE 1",
        settings={"mutations_sync": 2},
    )

    # `DETACH PART` leaves an empty part covering the detached range, so every part lookup after it
    # must skip the empty parts.
    mutated_part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='no_codec_file' AND active AND rows > 0"
    ).strip()
    assert (
        node4.exec_in_container(
            ["cat", f"{data_path}{mutated_part_name}/default_compression_codec.txt"]
        ).strip()
        == "UNKNOWN"
    )

    node4.query("DETACH TABLE no_codec_file")
    node4.query("ATTACH TABLE no_codec_file")
    assert node4.query("SELECT data FROM no_codec_file ORDER BY key") == "Hello world!\nGoodbye world!\n"

    node4.query("DROP TABLE no_codec_file SYNC")


def test_full_rewrite_mutation_records_exact_codec(start_cluster):
    # Counterpart of `test_missing_codec_file_fails_closed_for_modern_part`, which pins the
    # column-only mutation path. The `UNKNOWN` provenance marker exists because that path hardlinks
    # the columns it does not rewrite and so inherits their unknown codec. A full rewrite - the path
    # a `Compact` part takes - re-encodes every column with the codec it picks, so that codec is an
    # exact fact about the new part and has to be recorded as one. Propagating `UNKNOWN` here would
    # leave a part that is now fully described permanently unknown, and an unknown codec makes the
    # recompression TTL selector reconsider the part on every pass.
    #
    # `min_bytes_for_wide_part` is pinned high for the same reason the sibling test pins it to zero:
    # the part format decides which mutation path runs, so it must not be left to the default.
    node4.query(
        """
    CREATE TABLE unknown_codec_full_rewrite (
        key UInt64 CODEC(ZSTD(1)),
        data String CODEC(ZSTD(1))
    )
    ENGINE MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 1000000000
    """
    )

    node4.query("INSERT INTO unknown_codec_full_rewrite VALUES (1, 'Hello world')")

    part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='unknown_codec_full_rewrite' AND active AND rows > 0"
    ).strip()
    data_path = node4.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='unknown_codec_full_rewrite'"
    ).strip()

    node4.query(f"ALTER TABLE unknown_codec_full_rewrite DETACH PART '{part_name}'")
    node4.exec_in_container(
        [
            "python3",
            "-c",
            "import sys; open(sys.argv[1], 'w').write('UNKNOWN')",
            f"{data_path}detached/{part_name}/default_compression_codec.txt",
        ]
    )
    node4.query(f"ALTER TABLE unknown_codec_full_rewrite ATTACH PART '{part_name}'")

    assert (
        node4.query(
            "SELECT part_type, default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='unknown_codec_full_rewrite' AND active AND rows > 0"
        ).strip()
        == "Compact\tUNKNOWN"
    )

    node4.query(
        "ALTER TABLE unknown_codec_full_rewrite UPDATE data = concat(data, '!') WHERE 1",
        settings={"mutations_sync": 2},
    )

    mutated_part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='unknown_codec_full_rewrite' AND active AND rows > 0"
    ).strip()
    recorded_codec = node4.exec_in_container(
        ["cat", f"{data_path}{mutated_part_name}/default_compression_codec.txt"]
    ).strip()
    assert recorded_codec.startswith("CODEC(") and recorded_codec.endswith(")")

    # The system table must agree with the file: it reports the codec without the `CODEC(...)`
    # wrapper, and `UNKNOWN` only when the part has no authoritative codec.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='unknown_codec_full_rewrite' AND active AND rows > 0"
        ).strip()
        == recorded_codec.removeprefix("CODEC(").removesuffix(")")
    )

    node4.query("DETACH TABLE unknown_codec_full_rewrite")
    node4.query("ATTACH TABLE unknown_codec_full_rewrite")
    assert node4.query("SELECT data FROM unknown_codec_full_rewrite") == "Hello world!\n"

    node4.query("DROP TABLE unknown_codec_full_rewrite SYNC")


def test_malformed_codec_file_fails_closed_for_modern_part(start_cluster):
    # A `default_compression_codec.txt` file can be present but unparseable (corrupted or truncated,
    # for example after an interrupted detach/copy/restore). When every column has an explicit
    # `CODEC`, no data records the part default and a modern `checksums.txt` cannot reconstruct it.
    node4.query(
        """
    CREATE TABLE malformed_codec_file (
        key UInt64 CODEC(ZSTD(1)),
        data String CODEC(ZSTD(1))
    )
    ENGINE MergeTree ORDER BY tuple()
    """
    )

    node4.query("INSERT INTO malformed_codec_file VALUES (1, 'Hello world')")
    node4.query("INSERT INTO malformed_codec_file VALUES (2, 'Goodbye world')")
    node4.query("OPTIMIZE TABLE malformed_codec_file FINAL")

    part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='malformed_codec_file' AND active"
    ).strip()

    node4.query(f"ALTER TABLE malformed_codec_file DETACH PART '{part_name}'")

    data_path = node4.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='malformed_codec_file'"
    ).strip()
    # Truncate the codec file to empty (an unparseable "CODEC" line) without a shell.
    node4.exec_in_container(
        ["cp", "/dev/null", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )

    with pytest.raises(Exception, match="Cannot recover the default compression codec"):
        node4.query(f"ALTER TABLE malformed_codec_file ATTACH PART '{part_name}'")

    node4.query("DROP TABLE malformed_codec_file SYNC")


def test_missing_codec_and_checksums_files_fail_closed(start_cluster):
    # A part can lose *both* `default_compression_codec.txt` and `checksums.txt` (for example after a
    # partial detach/copy/restore that dropped metadata files). On such a part every column here has an
    # explicit `CODEC`, so no column proves the default codec. A regenerated `checksums.txt` is not
    # provenance, so attachment must fail rather than choose an arbitrary default.
    node4.query(
        """
    CREATE TABLE no_codec_no_checksums (
        key UInt64 CODEC(ZSTD(1)),
        data String CODEC(ZSTD(1))
    )
    ENGINE MergeTree ORDER BY tuple()
    """
    )

    # Two inserts and a merge, so the part whose metadata we strip is a merged part. `checksums.txt`
    # is always compressed with the current built-in default codec (`ZSTD(3)`), so both the original
    # and the regenerated file are a ZSTD frame - the recovery would infer `ZSTD(1)` from it if it
    # trusted a regenerated file, so `LZ4` in the assertion below is only reachable with the fix.
    node4.query("INSERT INTO no_codec_no_checksums VALUES (1, 'Hello world')")
    node4.query("INSERT INTO no_codec_no_checksums VALUES (2, 'Goodbye world')")
    node4.query("OPTIMIZE TABLE no_codec_no_checksums FINAL")

    part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='no_codec_no_checksums' AND active"
    ).strip()

    node4.query(f"ALTER TABLE no_codec_no_checksums DETACH PART '{part_name}'")

    data_path = node4.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='no_codec_no_checksums'"
    ).strip()
    # Remove both provenance files, leaving nothing on disk that records the write-time codec.
    node4.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )
    node4.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/checksums.txt"]
    )

    with pytest.raises(Exception, match="Cannot recover the default compression codec"):
        node4.query(f"ALTER TABLE no_codec_no_checksums ATTACH PART '{part_name}'")

    node4.query("DROP TABLE no_codec_no_checksums SYNC")


def test_default_codec_recovered_from_explicit_default_codec_column(start_cluster):
    # A column declared with an explicit `CODEC(Default)` is still compressed with the part's default
    # codec, exactly like a column with no CODEC at all, so its `.bin` proves the default codec family.
    # `hasCompressionCodec` is true for such a column (it carries a codec descriptor), so before the
    # fix the recovery treated `CODEC(Default)` like an explicit non-default codec and skipped it,
    # dropping to the lossy `checksums.txt` fallback and mislabeling the part.
    #
    # Here `key` has an explicit non-default codec (skipped) and `data` has `CODEC(Default)`, so no
    # plain no-codec column exists. On `node4` a small merged part uses the size-aware default `LZ4`,
    # so the `data` column `.bin` is an `LZ4` frame, while `checksums.txt` is always a `ZSTD(3)` frame.
    # After dropping the codec file the recovered default must be `LZ4` (read from the `CODEC(Default)`
    # column data) - `ZSTD(1)` (from the checksums fallback) is the pre-fix, wrong result.
    #
    # The part must be Wide: attributing a frame of the shared Compact `data.bin` to one column is
    # not possible with mixed codecs (see `test_default_codec_not_misattributed_in_compact_part`).
    node4.query(
        """
    CREATE TABLE explicit_default_codec_column (
        key UInt64 CODEC(ZSTD(1)),
        data String CODEC(Default)
    )
    ENGINE MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
    """
    )

    # Two inserts and a merge, so the part whose codec file we drop is a merged part.
    node4.query("INSERT INTO explicit_default_codec_column VALUES (1, 'Hello world')")
    node4.query("INSERT INTO explicit_default_codec_column VALUES (2, 'Goodbye world')")
    node4.query("OPTIMIZE TABLE explicit_default_codec_column FINAL")

    part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='explicit_default_codec_column' AND active"
    ).strip()

    node4.query(f"ALTER TABLE explicit_default_codec_column DETACH PART '{part_name}'")

    data_path = node4.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='explicit_default_codec_column'"
    ).strip()
    node4.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )

    node4.query(f"ALTER TABLE explicit_default_codec_column ATTACH PART '{part_name}'")

    assert node4.query("SELECT COUNT() FROM explicit_default_codec_column") == "2\n"

    # The `CODEC(Default)` column proves the `LZ4` family, but its frame does not preserve all codec
    # parameters. Do not present that recovery as authoritative part metadata.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='explicit_default_codec_column' AND active"
        ).strip()
        == "UNKNOWN"
    )

    node4.query("DROP TABLE explicit_default_codec_column SYNC")


def test_default_codec_recovered_from_pipeline_default_codec_column(start_cluster):
    # `Default` can also be the generic-compression stage of a codec pipeline (`CODEC(Delta,
    # Default)`). Such a column's `.bin` is a `Multiple` chain whose generic stage is the part's
    # default codec, so it proves the default codec family just like a bare `CODEC(Default)` column.
    #
    # `key` has an explicit non-default codec, `data` has `CODEC(Delta, Default)`, and no plain
    # no-codec column exists. On `node4` the small merged part uses the size-aware default `LZ4`, so
    # after dropping the codec file the recovery must extract `LZ4` from the `data` column's
    # `Multiple(Delta, LZ4)` frame - `ZSTD(1)` (from the lossy `checksums.txt` fallback) is the
    # pre-fix, wrong result.
    node4.query(
        """
    CREATE TABLE pipeline_default_codec_column (
        key UInt64 CODEC(ZSTD(1)),
        data UInt64 CODEC(Delta, Default)
    )
    ENGINE MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
    """
    )

    # Two inserts and a merge, so the part whose codec file we drop is a merged part.
    node4.query("INSERT INTO pipeline_default_codec_column VALUES (1, 1)")
    node4.query("INSERT INTO pipeline_default_codec_column VALUES (2, 2)")
    node4.query("OPTIMIZE TABLE pipeline_default_codec_column FINAL")

    part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='pipeline_default_codec_column' AND active"
    ).strip()

    node4.query(f"ALTER TABLE pipeline_default_codec_column DETACH PART '{part_name}'")

    data_path = node4.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='pipeline_default_codec_column'"
    ).strip()
    node4.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )

    node4.query(f"ALTER TABLE pipeline_default_codec_column ATTACH PART '{part_name}'")

    assert node4.query("SELECT COUNT() FROM pipeline_default_codec_column") == "2\n"

    # The generic-compression stage proves the `LZ4` family but not authoritative part metadata.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='pipeline_default_codec_column' AND active"
        ).strip()
        == "UNKNOWN"
    )

    node4.query("DROP TABLE pipeline_default_codec_column SYNC")


def test_default_codec_recovered_from_lz4hc_part(start_cluster):
    # `LZ4` and `LZ4HC` share the same on-disk method byte (`0x82`, see `CompressionInfo.h`), so a
    # part whose default codec was `LZ4HC(N)` reads back as plain `LZ4` from its column data - the
    # frame preserves neither the `HC` variant nor the level. The recovery must degrade to the `LZ4`
    # family guess (and internally mark `default_codec` approximate - the same unconditional marking
    # as for the `ZSTD(3)` -> `ZSTD(1)` case above, so e.g. `TTLRecompressMergeSelector` reconsiders
    # the part instead of trusting the weaker codec).
    node4.query(
        """
    CREATE TABLE lz4hc_default_codec (
        key UInt64,
        data String
    )
    ENGINE MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'LZ4HC(5)'
    """
    )

    # Two inserts and a merge, so the part whose codec file we drop is a merged part.
    node4.query("INSERT INTO lz4hc_default_codec VALUES (1, 'Hello world')")
    node4.query("INSERT INTO lz4hc_default_codec VALUES (2, 'Goodbye world')")
    node4.query("OPTIMIZE TABLE lz4hc_default_codec FINAL")

    part_name = node4.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='lz4hc_default_codec' AND active"
    ).strip()

    # While `default_compression_codec.txt` is still present, the exact write-time default is known.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='lz4hc_default_codec' AND active"
        ).strip()
        == "LZ4HC(5)"
    )

    node4.query(f"ALTER TABLE lz4hc_default_codec DETACH PART '{part_name}'")

    data_path = node4.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='lz4hc_default_codec'"
    ).strip()
    node4.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )

    node4.query(f"ALTER TABLE lz4hc_default_codec ATTACH PART '{part_name}'")

    assert node4.query("SELECT COUNT() FROM lz4hc_default_codec") == "2\n"

    # The shared method byte only proves the `LZ4` family; the `HC` variant and level are lost, so
    # the system table must expose the missing provenance.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='lz4hc_default_codec' AND active"
        ).strip()
        == "UNKNOWN"
    )

    node4.query("DROP TABLE lz4hc_default_codec SYNC")


def test_mixed_codec_compact_part_fails_closed_without_codec_file(start_cluster):
    # In a Compact part all columns share a single `data.bin`, and the recovery reads that file's
    # *first* frame, which belongs to whichever column was written first - not necessarily to the
    # column being inspected. With mixed codecs the frame cannot be attributed to a column, so the
    # column-proven recovery must be skipped.
    #
    # On `node5` the default codec is pinned to `ZSTD(3)` for parts of any size, so the no-codec
    # `data` column is a `ZSTD` frame, while the first column `key` carries an explicit `LZ4` codec
    # and owns the first frame of the shared `data.bin`. The modern `checksums.txt` frame records the
    # built-in codec rather than the selected part default, so a missing codec file must fail closed.
    node5.query(
        """
    CREATE TABLE mixed_codec_compact_part (
        key UInt64 CODEC(LZ4),
        data String
    )
    ENGINE MergeTree ORDER BY tuple()
    """
    )

    # Two inserts and a merge, so the part whose codec file we drop is a merged part.
    node5.query("INSERT INTO mixed_codec_compact_part VALUES (1, 'Hello world')")
    node5.query("INSERT INTO mixed_codec_compact_part VALUES (2, 'Goodbye world')")
    node5.query("OPTIMIZE TABLE mixed_codec_compact_part FINAL")

    part_name = node5.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='mixed_codec_compact_part' AND active"
    ).strip()
    part_type = node5.query(
        "SELECT part_type FROM system.parts WHERE database='default' AND table='mixed_codec_compact_part' AND active"
    ).strip()
    assert part_type == "Compact"

    node5.query(f"ALTER TABLE mixed_codec_compact_part DETACH PART '{part_name}'")

    data_path = node5.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='mixed_codec_compact_part'"
    ).strip()
    node5.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )

    with pytest.raises(Exception, match="Cannot recover the default compression codec"):
        node5.query(f"ALTER TABLE mixed_codec_compact_part ATTACH PART '{part_name}'")

    node5.query("DROP TABLE mixed_codec_compact_part SYNC")


def test_default_codec_approximate_when_recovered_from_column_data(start_cluster):
    # The recovery cases above use tables where *every* column has an explicit CODEC, so no column
    # proves the default codec and `detectDefaultCompressionCodec` recovers it from `checksums.txt`.
    # This test covers the other branch: when a column has *no* explicit CODEC, its `.bin` is
    # compressed with the part's default codec, and the recovery reads that column frame directly.
    #
    # `getCompressionCodecForFile` reconstructs the codec from the compressed frame's method byte
    # only. The method byte identifies the codec *family* but not its numeric parameters, so a column
    # written with `ZSTD(3)` comes back as `ZSTD(1)`. `node5` pins the default codec to `ZSTD(3)` for
    # parts of any size (see `configs/force_zstd3.xml`), so we can produce a small default-coded
    # `ZSTD(3)` part and observe the level being lost on recovery. Because the recovered `default_codec`
    # no longer matches the part's real codec, it must be treated as approximate (its level is a
    # default guess) so consumers such as `TTLRecompressMergeSelector` do not trust the guessed level.
    node5.query(
        """
    CREATE TABLE approximate_default_codec (
        key UInt64,
        data String
    )
    ENGINE MergeTree ORDER BY tuple()
    """
    )

    # Two inserts and a merge, so the part whose codec file we drop is a merged part.
    node5.query("INSERT INTO approximate_default_codec VALUES (1, 'Hello world')")
    node5.query("INSERT INTO approximate_default_codec VALUES (2, 'Goodbye world')")
    node5.query("OPTIMIZE TABLE approximate_default_codec FINAL")

    part_name = node5.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='approximate_default_codec' AND active"
    ).strip()

    # While `default_compression_codec.txt` is still present, the exact write-time default is known.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='approximate_default_codec' AND active"
        ).strip()
        == "ZSTD(3)"
    )

    node5.query(f"ALTER TABLE approximate_default_codec DETACH PART '{part_name}'")

    data_path = node5.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='approximate_default_codec'"
    ).strip()
    # Removing `checksums.txt` makes the loader regenerate it. Recovery must still inspect the
    # default-coded column first: the regenerated file has no write-time codec provenance.
    node5.exec_in_container(
        [
            "rm",
            f"{data_path}detached/{part_name}/default_compression_codec.txt",
            f"{data_path}detached/{part_name}/checksums.txt",
        ]
    )

    node5.query(f"ALTER TABLE approximate_default_codec ATTACH PART '{part_name}'")

    assert node5.query("SELECT COUNT() FROM approximate_default_codec") == "2\n"

    # The `data` column's `.bin` proves the codec *family* (ZSTD), but the frame does not store the
    # level, so the recovered default comes back internally as `ZSTD(1)` rather than the real
    # `ZSTD(3)`. The system table must not present that best-effort guess as authoritative metadata.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='approximate_default_codec' AND active"
        ).strip()
        == "UNKNOWN"
    )

    node5.query("DROP TABLE approximate_default_codec SYNC")


def test_missing_codec_file_fails_closed_for_modern_part_after_codec_alter(start_cluster):
    # `checksums.txt` records the built-in codec, not the part default. After an ALTER, the current
    # column metadata cannot prove the old part default either. For a modern part whose mandatory
    # codec file was removed, `ATTACH PART` must fail rather than report a false default codec.
    node1.query(
        """
    CREATE TABLE codec_alter_fence (
        key UInt64 CODEC(ZSTD(1)),
        data String CODEC(LZ4)
    )
    ENGINE = ReplicatedMergeTree('/codec_alter_fence', '1') ORDER BY tuple()
    """
    )

    node1.query("INSERT INTO codec_alter_fence VALUES (1, 'Hello world')")

    part_name = node1.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='codec_alter_fence' AND active"
    ).strip()

    # While `default_compression_codec.txt` is present, the exact write-time default is known: the
    # part is smaller than 1024 bytes, so the `<compression>` config on this node picks `ZSTD(10)`.
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='codec_alter_fence' AND active"
        ).strip()
        == "ZSTD(10)"
    )

    node1.query(f"ALTER TABLE codec_alter_fence DETACH PART '{part_name}'")

    data_path = node1.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='codec_alter_fence'"
    ).strip()
    node1.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )

    # Bump the table metadata while the part is detached: in the *current* metadata `data` is now
    # default-coded, while in the part it is an explicitly-coded `LZ4` column.
    node1.query("ALTER TABLE codec_alter_fence MODIFY COLUMN data REMOVE CODEC")

    with pytest.raises(Exception, match="Cannot recover the default compression codec"):
        node1.query(f"ALTER TABLE codec_alter_fence ATTACH PART '{part_name}'")

    node1.query("DROP TABLE codec_alter_fence SYNC")


def test_default_codec_provenance_survives_column_only_mutation(start_cluster):
    # A part whose default codec could only be recovered approximately must not reuse that guess to
    # encode columns rewritten by a column-only mutation. The writer chooses the current table / TTL
    # policy independently. Because the other columns are hardlinked, the descendant still has no
    # authoritative part-wide codec and must not record one in `default_compression_codec.txt`.
    #
    # `node5` pins `ZSTD(3)` for parts of any size, and the recovery reconstructs the codec from the
    # column frame's method byte, which does not store the level - so the guess is `ZSTD(1)`, exactly
    # the codec of the `RECOMPRESS` TTL below. Trusting it would make the recompression look like a
    # no-op and the part would never be recompressed.
    node5.query(
        """
    CREATE TABLE codec_provenance_mutation (
        key UInt64,
        ts DateTime,
        data String,
        extra UInt64
    )
    ENGINE = MergeTree ORDER BY key
    TTL ts + INTERVAL 1 SECOND RECOMPRESS CODEC(ZSTD(1))
    SETTINGS min_bytes_for_wide_part = 0, merge_with_recompression_ttl_timeout = 0
    """
    )

    node5.query(
        "ALTER TABLE codec_provenance_mutation ADD PROJECTION by_data (SELECT key, data ORDER BY data)"
    )

    # The recompression TTL is due for the row inserted below, so hold TTL merges back until the setup
    # is complete. This blocker does not affect mutations.
    node5.query("SYSTEM STOP TTL MERGES codec_provenance_mutation")

    node5.query(
        "INSERT INTO codec_provenance_mutation VALUES (1, now() - INTERVAL 1 DAY, 'Hello world', 1)"
    )

    part_name = node5.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='codec_provenance_mutation' AND active AND rows > 0"
    ).strip()

    # While `default_compression_codec.txt` is present, the exact write-time default is known.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='codec_provenance_mutation' AND active AND rows > 0"
        ).strip()
        == "ZSTD(3)"
    )

    data_path = node5.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='codec_provenance_mutation'"
    ).strip()

    node5.query(f"ALTER TABLE codec_provenance_mutation DETACH PART '{part_name}'")
    node5.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )
    node5.query(f"ALTER TABLE codec_provenance_mutation ATTACH PART '{part_name}'")

    # `ATTACH PART` gives the part a new block number, so re-read its name.
    part_name = node5.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='codec_provenance_mutation' AND active AND rows > 0"
    ).strip()

    # The level was lost by the recovery. The internal estimate is `ZSTD(1)`, but the system table
    # must not present that lossy estimate as authoritative part metadata.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='codec_provenance_mutation' AND active AND rows > 0"
        ).strip()
        == "UNKNOWN"
    )

    # A column-only mutation: only `extra` is rewritten, everything else is hardlinked.
    node5.query(
        "ALTER TABLE codec_provenance_mutation UPDATE extra = extra + 1 WHERE 1",
        settings={"mutations_sync": 2},
    )

    mutated_part_name = node5.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='codec_provenance_mutation' AND active AND rows > 0"
    ).strip()
    assert mutated_part_name != part_name

    # The writer must choose the due `RECOMPRESS` TTL codec instead of reusing the recovered
    # `ZSTD(1)` estimate merely because it happens to match it. It cannot persist that codec as
    # authoritative metadata, because the untouched columns are still hardlinked with `ZSTD(3)`.
    # It writes the explicit unknown marker instead. In particular, this avoids treating a fresh
    # modern `checksums.txt` as legacy provenance when every hardlinked column has an explicit codec.
    assert (
        node5.exec_in_container(
            ["cat", f"{data_path}{mutated_part_name}/default_compression_codec.txt"]
        ).strip()
        == "UNKNOWN"
    )

    # A projection rebuilt by the column-only mutation chooses its codec independently instead of
    # inheriting the approximate parent estimate. On this node that is the configured `ZSTD(3)`.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.projection_parts WHERE database='default' "
            "AND table='codec_provenance_mutation' AND name='by_data' AND active AND rows > 0"
        ).strip()
        == "ZSTD(3)"
    )

    # Reloading recovers a part-wide estimate again rather than converting the codec chosen for
    # `extra` into metadata for the untouched columns.
    node5.query("DETACH TABLE codec_provenance_mutation")
    node5.query("ATTACH TABLE codec_provenance_mutation")

    assert node5.query("SELECT extra FROM codec_provenance_mutation").strip() == "2"

    # `DETACH PART` above left an empty part covering the detached range, and the reload brought it
    # back. It has no projections, so `OPTIMIZE ... FINAL` would refuse to merge it together with the
    # real part. Wait for the background cleanup to drop it.
    assert_eq_with_retry(
        node5,
        "SELECT count() FROM system.parts WHERE database='default' "
        "AND table='codec_provenance_mutation' AND active AND rows = 0",
        "0\n",
        retry_count=60,
        sleep_time=1,
    )

    # The due TTL must still be considered after the reload. Its full rewrite materializes every
    # default-coded column with `ZSTD(1)`, after which that codec becomes exact part-wide metadata.
    node5.query("SYSTEM START TTL MERGES codec_provenance_mutation")
    node5.query(
        "OPTIMIZE TABLE codec_provenance_mutation FINAL",
        settings={"optimize_throw_if_noop": 1},
    )

    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='codec_provenance_mutation' AND active AND rows > 0"
        ).strip()
        == "ZSTD(1)"
    )

    node5.query("DROP TABLE codec_provenance_mutation SYNC")


def test_projection_codec_is_not_inherited_from_approximate_guess(start_cluster):
    # A projection built for a part whose own default codec could only be recovered approximately must
    # not be compressed with that guess. Projections inherit the codec chosen for their parent part,
    # which is right while that codec is a fact, but a part that lost `default_compression_codec.txt`
    # only has a codec recovered from its compressed frames - and those do not store the level. The
    # projection is written here from scratch, so `finalizePartOnDisk` would record the inherited guess
    # in the projection's own `default_compression_codec.txt` as authoritative metadata and relabel the
    # projection permanently. The codec must therefore be chosen independently, exactly as a fresh
    # write does.
    #
    # `node5` pins `ZSTD(3)` for parts of any size, so the honest choice is `ZSTD(3)`, while the
    # recovery of the parent part can only guess `ZSTD(1)`.
    node5.query(
        """
    CREATE TABLE codec_provenance_projection (
        key UInt64,
        data String
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0
    """
    )

    node5.query(
        "INSERT INTO codec_provenance_projection VALUES (1, 'Hello world'), (2, 'Goodbye world')"
    )

    part_name = node5.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='codec_provenance_projection' AND active AND rows > 0"
    ).strip()

    data_path = node5.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='codec_provenance_projection'"
    ).strip()

    # Lose the part's codec file, the way a detach / copy / restore of an old part can.
    node5.query(f"ALTER TABLE codec_provenance_projection DETACH PART '{part_name}'")
    node5.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )
    node5.query(f"ALTER TABLE codec_provenance_projection ATTACH PART '{part_name}'")

    # `ATTACH PART` gives the part a new block number, so re-read its name.
    part_name = node5.query(
        "SELECT name FROM system.parts WHERE database='default' AND table='codec_provenance_projection' AND active AND rows > 0"
    ).strip()

    # The level was lost by the recovery: the part's codec is now only a guess and is reported as
    # unknown rather than exposing the internal estimate.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='codec_provenance_projection' AND active AND rows > 0"
        ).strip()
        == "UNKNOWN"
    )

    # Build a projection for that part: none of its columns declare a `CODEC`, so all of them are
    # written with the codec the projection part gets.
    node5.query(
        "ALTER TABLE codec_provenance_projection ADD PROJECTION by_data (SELECT key, data ORDER BY data)"
    )
    node5.query(
        "ALTER TABLE codec_provenance_projection MATERIALIZE PROJECTION by_data",
        settings={"mutations_sync": 2},
    )

    # The projection is compressed with the codec chosen for it here, not with the parent's guess - so
    # its own codec file stays an exact statement about its data.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.projection_parts WHERE database='default' "
            "AND table='codec_provenance_projection' AND name='by_data' AND active AND rows > 0"
        ).strip()
        == "ZSTD(3)"
    )

    # Reload from disk: what was recorded for the projection must survive the round trip, and the
    # projection must still be readable with the codec it was written with.
    node5.query("DETACH TABLE codec_provenance_projection")
    node5.query("ATTACH TABLE codec_provenance_projection")

    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.projection_parts WHERE database='default' "
            "AND table='codec_provenance_projection' AND name='by_data' AND active AND rows > 0"
        ).strip()
        == "ZSTD(3)"
    )
    assert (
        node5.query(
            "SELECT data FROM codec_provenance_projection ORDER BY data",
            settings={"optimize_use_projections": 1, "force_optimize_projection": 1},
        ).strip()
        == "Goodbye world\nHello world"
    )

    node5.query("DROP TABLE codec_provenance_projection SYNC")


def test_projection_rebuild_with_multiple_temporary_parts_keeps_recompression_codec(start_cluster):
    # Projection rebuilding first writes temporary projection parts, then merges those parts into the
    # projection attached to the parent part. The threshold is set in node6's startup profile: the
    # background merge context does not use a client-side `SET`. This creates 37 temporary parts,
    # which exercises the `MergeProjectionPartsTask` sub-merge path rather than the single-part path.
    node6.query(
        """
    CREATE TABLE projection_rebuild_multiple_parts (
        ts DateTime,
        x UInt64,
        PROJECTION p (SELECT x ORDER BY x)
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    TTL ts + INTERVAL 1 SECOND RECOMPRESS CODEC(NONE)
    SETTINGS
        materialize_projections_on_insert = 0,
        materialize_projections_on_merge = 1,
        enable_adaptive_codec_selection = 1,
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        merge_with_recompression_ttl_timeout = 0
    """
    )

    node6.query("SYSTEM STOP TTL MERGES projection_rebuild_multiple_parts")
    node6.query(
        "INSERT INTO projection_rebuild_multiple_parts "
        "SELECT now() - INTERVAL 1 DAY, number FROM numbers(3700)"
    )

    # Do not use `OPTIMIZE FINAL`: its second merge takes the single-part path and masks this bug.
    node6.query("SYSTEM START TTL MERGES projection_rebuild_multiple_parts")
    assert_eq_with_retry(
        node6,
        "SELECT default_compression_codec FROM system.parts "
        "WHERE database = 'default' AND table = 'projection_rebuild_multiple_parts' "
        "AND active AND rows > 0",
        "NONE\n",
        retry_count=60,
    )

    # `NONE` must be used for the projection bytes too. If the projection sub-merge loses the
    # explicit recompression codec, adaptive selection compresses monotonic `x` with `T64`.
    assert (
        node6.query(
            "SELECT min(data_compressed_bytes >= data_uncompressed_bytes) "
            "FROM system.projection_parts_columns "
            "WHERE database = 'default' AND table = 'projection_rebuild_multiple_parts' "
            "AND active AND column = 'x'"
        ).strip()
        == "1"
    )

    node6.query("DROP TABLE projection_rebuild_multiple_parts SYNC")
