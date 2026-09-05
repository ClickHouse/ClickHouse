# A marks file that a part's `checksums.txt` lists but that is absent on disk must surface a
# typed `NO_FILE_IN_DATA_PART` naming the part, the file, whether the file is listed in the
# checksums, and the on-disk listing, instead of an opaque
# `std::filesystem::filesystem_error: file_size: No such file or directory`.
#
# Converted from the stateless test 03810_missing_marks_file_error.sh.
#
# These tests delete a real marks file out of a live part directory, which stateless tests may
# not do (they run against arbitrary server configurations: object storage, shared MergeTree,
# encrypted disks). The original carried `no-object-storage`, `no-shared-merge-tree` and
# `no-replicated-database` tags for that reason, plus `no-parallel` because it issues the
# server-wide `SYSTEM DROP MARK CACHE`; one plain local-disk node satisfies all four by
# construction.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")

# Wide parts keep one marks file per column stream (`.cmrk2`/`.mrk2`, or `.cmrk`/`.mrk` for
# non-adaptive granularity); compact parts keep a single "data" marks file (`.cmrk4`/`.mrk4`,
# `.cmrk3`/`.mrk3`). Match by extension rather than by `<column>.<ext>`: with
# `replace_long_file_name_to_hash` in play a stream's basename can be a hash, but the marks
# extension never is.
MARKS_EXTENSIONS = ("cmrk2", "mrk2", "cmrk", "mrk", "cmrk4", "mrk4", "cmrk3", "mrk3")

# `ratio_of_defaults_for_sparse_serialization = 1` keeps dense per-column marks: with sparse
# serialization a column gains a `.sparse.idx` substream and a missing main marks file is caught
# by the checksum consistency check, a different and already-typed error, instead of by the marks
# loader under test. `min_bytes_for_full_part_storage = 0` keeps Full part storage, because packed
# storage bundles the part into one `data.packed` archive and leaves no separate marks file to
# remove. `prewarm_mark_cache = 0` keeps the marks off the cache so they are read from disk.
COMMON_SETTINGS = (
    "replace_long_file_name_to_hash = 0, prewarm_mark_cache = 0, "
    "ratio_of_defaults_for_sparse_serialization = 1, min_bytes_for_full_part_storage = 0"
)
WIDE_SETTINGS = (
    f"min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, {COMMON_SETTINGS}"
)
# The 1000-row insert stays below both thresholds, so the part is written compact.
COMPACT_SETTINGS = f"min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, {COMMON_SETTINGS}"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def exec_root(cmd):
    return node.exec_in_container(["bash", "-c", cmd], privileged=True, user="root")


def active_part_path(table):
    path = node.query(
        f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active LIMIT 1"
    ).strip()
    assert path.startswith("/"), f"path is relative: {path}"
    return path.rstrip("/")


def remove_all_marks_files(part_path):
    # Remove every marks file in the part, so the marks of whichever column the case below reads
    # or loads are gone. Returns the count so a layout change cannot leave the case asserting
    # against an intact part.
    globs = " ".join(f"{part_path}/*.{ext}" for ext in MARKS_EXTENSIONS)
    removed = exec_root(
        f'shopt -s nullglob; n=0; for f in {globs}; do rm -f "$f"; n=$((n + 1)); done; echo "$n"'
    ).strip()
    assert (
        removed != "0"
    ), f"no marks file in {part_path}: {exec_root(f'ls -1 {part_path}')}"
    return int(removed)


def create_and_fill(table, settings):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"CREATE TABLE {table} (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS {settings}"
    )
    node.query(f"INSERT INTO {table} SELECT number, number FROM numbers(1000)")


def drop_table(table):
    # Dropping a table whose marks file was removed logs a CANNOT_UNLINK error during directory
    # cleanup, which is expected here and must not reach the client.
    node.query(
        f"DROP TABLE IF EXISTS {table} SYNC", settings={"send_logs_level": "none"}
    )


def assert_diagnostic_logged(table):
    # `reason` is `broken-on-start` whether the load failed with the typed diagnostic or with the
    # opaque `std::filesystem` error, so it cannot tell the two apart. The load path reports
    # through the log rather than to a client, so this text_log assertion is what actually
    # distinguishes them. The logger is `default.<table> (<uuid>)` under Atomic and
    # `default.<table>` under Ordinary; the pattern matches both.
    node.query("SYSTEM FLUSH LOGS text_log")
    assert node.query(f"""SELECT count() > 0 FROM system.text_log
                WHERE event_time > now() - INTERVAL 5 MINUTE
                  AND logger_name LIKE '%default.{table}%'
                  AND message LIKE '%does not exist on disk in part%'
                  AND message LIKE '%listed in the part%checksums%'""") == "1\n"


def test_query_read_path(started_cluster):
    # `MergeTreeMarksLoader::loadMarksImpl`: the marks are loaded lazily while a query reads the
    # column, so the error reaches the client.
    table = "t_missing_marks"
    try:
        create_and_fill(table, WIDE_SETTINGS)
        remove_all_marks_files(active_part_path(table))
        node.query("SYSTEM DROP MARK CACHE")

        # Column b's marks file is listed in the part's checksums, so the message says "listed".
        error = node.query_and_get_error(f"SELECT sum(b) FROM {table}")
        assert "NO_FILE_IN_DATA_PART" in error, error
        assert "does not exist on disk in part" in error, error
        assert "is listed in the part's checksums" in error, error
    finally:
        drop_table(table)


def test_wide_index_granularity_load_path(started_cluster):
    # `MergeTreeDataPartWide::loadIndexGranularity`: index granularity is read from the first
    # column's marks file when the part is loaded, before any query reaches the marks loader.
    table = "t_missing_granularity_marks"
    try:
        create_and_fill(table, WIDE_SETTINGS)
        part_path = active_part_path(table)

        # Detaching unloads the part, so ATTACH below re-reads index granularity from disk.
        node.query(f"DETACH TABLE {table}")
        remove_all_marks_files(part_path)
        node.query(f"ATTACH TABLE {table}", settings={"send_logs_level": "none"})

        assert (
            node.query(
                f"SELECT reason FROM system.detached_parts WHERE database = 'default' AND table = '{table}'"
            )
            == "broken-on-start\n"
        )
        assert_diagnostic_logged(table)
    finally:
        drop_table(table)


def test_compact_index_granularity_load_path(started_cluster):
    # `MergeTreeDataPartCompact::loadIndexGranularity`: a compact part keeps every column's marks
    # in a single "data" marks file, read on load through `readFileIfExists` rather than the wide
    # path's `existsFile`/`readFile` pair.
    table = "t_missing_compact_marks"
    try:
        create_and_fill(table, COMPACT_SETTINGS)
        assert (
            node.query(
                f"SELECT part_type FROM system.parts WHERE database = 'default' AND table = '{table}' AND active LIMIT 1"
            )
            == "Compact\n"
        )
        part_path = active_part_path(table)

        node.query(f"DETACH TABLE {table}")
        remove_all_marks_files(part_path)
        node.query(f"ATTACH TABLE {table}", settings={"send_logs_level": "none"})

        assert (
            node.query(
                f"SELECT reason FROM system.detached_parts WHERE database = 'default' AND table = '{table}'"
            )
            == "broken-on-start\n"
        )
        assert_diagnostic_logged(table)
    finally:
        drop_table(table)
