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


def test_default_codec_recovered_from_checksums_when_codec_file_missing(start_cluster):
    # A part can be missing `default_compression_codec.txt` because it is genuinely legacy (that file
    # was introduced long ago) or because a modern part lost it, for example during
    # detach/copy/restore. When every column has an explicit CODEC, no column proves the default
    # codec, so `IMergeTreeDataPart::detectDefaultCompressionCodec` cannot read it from a column
    # `.bin` and must recover it. It recovers the codec from `checksums.txt`, whose modern format is
    # compressed with the default codec effective when the part was written, so a legacy `LZ4` part
    # is not upgraded to the new `ZSTD(3)` default and, just as importantly, a modern part that only
    # lost its codec file is not silently downgraded to `LZ4` (which would also propagate through the
    # projection codec inheritance in `MergeTask` / `MutateTask`).
    #
    # Here the part is written and then merged under the new `ZSTD(3)` default, so its `checksums.txt`
    # is a ZSTD frame; after we drop the codec file from the merged part the recovered default must
    # stay a ZSTD codec (the frame does not store the level, so it comes back as `ZSTD(1)`) rather
    # than falling back to `LZ4`.
    node4.query(
        """
    CREATE TABLE no_codec_file (
        key UInt64 CODEC(ZSTD(1)),
        data String CODEC(ZSTD(1))
    )
    ENGINE MergeTree ORDER BY tuple()
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

    node4.query(f"ALTER TABLE no_codec_file ATTACH PART '{part_name}'")

    assert node4.query("SELECT COUNT() FROM no_codec_file") == "2\n"

    # Recovered from the ZSTD-compressed `checksums.txt`; must not be silently downgraded to `LZ4`.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='no_codec_file' AND active"
        ).strip()
        == "ZSTD(1)"
    )

    node4.query("DROP TABLE no_codec_file SYNC")


def test_default_codec_recovered_from_checksums_when_codec_file_malformed(start_cluster):
    # A `default_compression_codec.txt` file can be present but unparseable (corrupted or truncated,
    # for example after an interrupted detach/copy/restore). Just like the missing-file case above,
    # when every column has an explicit CODEC no column proves the default codec, so
    # `IMergeTreeDataPart::loadDefaultCompressionCodec` must recover it from `checksums.txt` rather
    # than fall back to the current global default (`getDefaultCodec()`, which is a fixed `ZSTD(3)`):
    # the write-time codec family from the checksums frame is authoritative, while the current global
    # default can be a different family and would wrongly propagate through the projection codec
    # inheritance in `MergeTask` / `MutateTask`.
    #
    # The part is written and merged under the new `ZSTD(3)` default, so its `checksums.txt` is a ZSTD
    # frame. After we truncate the codec file to an empty (unparseable) file, the recovered default
    # must be the frame's `ZSTD(1)` (the frame does not store the level) - not the `ZSTD(3)` that the
    # raw global-default fallback would have produced.
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

    node4.query(f"ALTER TABLE malformed_codec_file ATTACH PART '{part_name}'")

    assert node4.query("SELECT COUNT() FROM malformed_codec_file") == "2\n"

    # Recovered from the ZSTD-compressed `checksums.txt`; must not fall back to the raw `ZSTD(3)`
    # global default (nor be downgraded to `LZ4`).
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='malformed_codec_file' AND active"
        ).strip()
        == "ZSTD(1)"
    )

    node4.query("DROP TABLE malformed_codec_file SYNC")


def test_default_codec_not_recovered_from_regenerated_checksums(start_cluster):
    # A part can lose *both* `default_compression_codec.txt` and `checksums.txt` (for example after a
    # partial detach/copy/restore that dropped metadata files). On such a part every column here has an
    # explicit CODEC, so no column proves the default codec and the recovery has nothing on disk that
    # records it.
    #
    # `loadColumnsChecksumsIndexes` runs `loadChecksums` before `loadDefaultCompressionCodec`. With
    # `checksums.txt` missing, `loadChecksums` regenerates it immediately, compressing it with the
    # *current* built-in default codec (`ZSTD(3)`), which has nothing to do with the codec the part was
    # written with. If `detectDefaultCompressionCodecFromChecksums` then read that freshly regenerated
    # frame it would infer the current default family (`ZSTD(1)`) and mislabel a legacy `LZ4` part as
    # ZSTD - the very provenance the recovery is meant to preserve. The regenerated frame must not be
    # trusted: with no genuine `checksums.txt` on disk the recovery must infer `LZ4`, exactly as for a
    # part that never had a `checksums.txt` at all.
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

    node4.query(f"ALTER TABLE no_codec_no_checksums ATTACH PART '{part_name}'")

    assert node4.query("SELECT COUNT() FROM no_codec_no_checksums") == "2\n"

    # `checksums.txt` was regenerated with the current `ZSTD(3)` default during ATTACH, but that frame
    # is not write-time provenance, so the recovery must fall back to `LZ4` rather than read `ZSTD(1)`
    # out of the regenerated frame.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='no_codec_no_checksums' AND active"
        ).strip()
        == "LZ4"
    )

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

    # Recovered from the `CODEC(Default)` column's `LZ4` frame, not from the `ZSTD(3)` `checksums.txt`.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='explicit_default_codec_column' AND active"
        ).strip()
        == "LZ4"
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

    # Recovered from the generic-compression stage of the `data` column's `Multiple` frame.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='pipeline_default_codec_column' AND active"
        ).strip()
        == "LZ4"
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

    # The shared method byte only proves the `LZ4` family; the `HC` variant and level are lost.
    assert (
        node4.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='lz4hc_default_codec' AND active"
        ).strip()
        == "LZ4"
    )

    node4.query("DROP TABLE lz4hc_default_codec SYNC")


def test_default_codec_not_misattributed_in_compact_part(start_cluster):
    # In a Compact part all columns share a single `data.bin`, and the recovery reads that file's
    # *first* frame, which belongs to whichever column was written first - not necessarily to the
    # column being inspected. With mixed codecs the frame cannot be attributed to a column, so the
    # column-proven recovery must be skipped in favor of the `checksums.txt` fallback.
    #
    # On `node5` the default codec is pinned to `ZSTD(3)` for parts of any size, so the no-codec
    # `data` column is a `ZSTD` frame, while the first column `key` carries an explicit `LZ4` codec
    # and owns the first frame of the shared `data.bin`. Before the fix the recovery attributed that
    # `LZ4` frame to `data` and confidently relabeled the part's default as `LZ4`; the correct result
    # is the `ZSTD(1)` family guess from the `checksums.txt` fallback (`checksums.txt` is written
    # with the built-in `ZSTD(3)` default, and the frame stores the method byte only).
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

    node5.query(f"ALTER TABLE mixed_codec_compact_part ATTACH PART '{part_name}'")

    assert node5.query("SELECT COUNT() FROM mixed_codec_compact_part") == "2\n"

    # The `checksums.txt` family guess, not the `key` column's `LZ4` frame.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='mixed_codec_compact_part' AND active"
        ).strip()
        == "ZSTD(1)"
    )

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
    node5.exec_in_container(
        ["rm", f"{data_path}detached/{part_name}/default_compression_codec.txt"]
    )

    node5.query(f"ALTER TABLE approximate_default_codec ATTACH PART '{part_name}'")

    assert node5.query("SELECT COUNT() FROM approximate_default_codec") == "2\n"

    # The `data` column's `.bin` proves the codec *family* (ZSTD), but the frame does not store the
    # level, so the recovered default comes back as `ZSTD(1)` rather than the real `ZSTD(3)`. The
    # recovery is therefore approximate; the level shown here is a best-effort guess.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='approximate_default_codec' AND active"
        ).strip()
        == "ZSTD(1)"
    )

    node5.query("DROP TABLE approximate_default_codec SYNC")


def test_default_codec_recovery_fenced_after_codec_alter(start_cluster):
    # `detectDefaultCompressionCodec` proves the part's default codec from the `.bin` frame of a
    # default-coded column, but it looks the codec declarations up in the *current* table metadata.
    # When column codecs were altered after the part was written, the current declarations do not
    # describe the part: a column that was explicitly coded at write time becomes "default-coded"
    # after `ALTER TABLE ... MODIFY COLUMN ... REMOVE CODEC`, and its frame would then be read as
    # proof of the part's default. The recovery must therefore distrust the column proof whenever
    # the part records a metadata version different from the current one (`ReplicatedMergeTree`
    # increments it on `ALTER`) and take the approximate `checksums.txt` fallback instead.
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

    node1.query(f"ALTER TABLE codec_alter_fence ATTACH PART '{part_name}'")

    assert node1.query("SELECT COUNT() FROM codec_alter_fence") == "1\n"

    # Without the metadata-version fence the recovery would read the `data` column's `LZ4` frame as
    # proof of the part's default and report `LZ4`. With the fence it must fall back to
    # `checksums.txt`, whose frame is compressed with the built-in write-time default - `ZSTD(3)`,
    # recovered without the level as `ZSTD(1)` - the same family as the real `ZSTD(10)` default.
    assert (
        node1.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='codec_alter_fence' AND active"
        ).strip()
        == "ZSTD(1)"
    )

    node1.query("DROP TABLE codec_alter_fence SYNC")


def test_default_codec_provenance_survives_column_only_mutation(start_cluster):
    # A part whose default codec could only be recovered approximately must stay "unknown" across a
    # column-only mutation. Such a mutation hardlinks most of the part and rewrites only the updated
    # columns, inheriting the source part's - guessed - default codec, and then used to serialize that
    # guess into the new part's `default_compression_codec.txt`. After the next load the descendant
    # looked exact, so `buildRecompressTTLInfo` started trusting the guess again and a wrong guess
    # could suppress a due `RECOMPRESS` TTL one generation later - the very bug that
    # `default_codec_is_approximate` prevents for the original part. The codec file must therefore not
    # be written at all when the codec is a guess: its absence is how "this part's default codec is not
    # recorded" is spelled on disk, so the recovery, and the flag with it, runs again on every load.
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

    # The level was lost by the recovery: a guess that happens to equal the `RECOMPRESS` TTL codec.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='codec_provenance_mutation' AND active AND rows > 0"
        ).strip()
        == "ZSTD(1)"
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

    # The guessed codec must not have been laundered into authoritative on-disk metadata.
    assert (
        node5.exec_in_container(
            [
                "bash",
                "-c",
                f"test -e {data_path}{mutated_part_name}/default_compression_codec.txt && echo present || echo absent",
            ]
        ).strip()
        == "absent"
    )

    # Reload the table from disk - this is where a persisted guess would start looking exact - and
    # thereby also restart TTL merges for the table.
    node5.query("DETACH TABLE codec_provenance_mutation")
    node5.query("ATTACH TABLE codec_provenance_mutation")

    assert node5.query("SELECT extra FROM codec_provenance_mutation").strip() == "2"

    # The rewritten part's codec is still unknown, so the selector must reconsider the part for the due
    # `RECOMPRESS` TTL even though the guessed `ZSTD(1)` equals the TTL's codec. The recompression is a
    # merge of this single part, so it bumps the part level; with the guess trusted it never happens.
    assert_eq_with_retry(
        node5,
        "SELECT count() FROM system.parts WHERE database='default' AND table='codec_provenance_mutation' "
        "AND active AND rows > 0 AND level > 0",
        "1",
    )

    # Recompressed with the TTL's codec, now recorded exactly.
    assert (
        node5.query(
            "SELECT default_compression_codec FROM system.parts "
            "WHERE database='default' AND table='codec_provenance_mutation' AND active AND rows > 0"
        ).strip()
        == "ZSTD(1)"
    )

    node5.query("DROP TABLE codec_provenance_mutation SYNC")
