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


def active_part(table):
    name, path = (
        node.query(
            f"SELECT name, path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active LIMIT 1"
        )
        .strip()
        .split("\t")
    )
    assert path.startswith("/"), f"path is relative: {path}"
    return name, path.rstrip("/")


def remove_all_marks_files(part_path):
    # Remove every marks file in the part, so the marks of whichever column the case below reads or
    # loads are gone. Returns their basenames: each case then asserts on the one its own code path
    # is supposed to name, which is what pins the case to that path rather than to any failure.
    globs = " ".join(f"{part_path}/*.{ext}" for ext in MARKS_EXTENSIONS)
    removed = exec_root(
        f'shopt -s nullglob; for f in {globs}; do echo "${{f##*/}}"; rm -f "$f"; done'
    ).split()
    assert removed, f"no marks file in {part_path}: {exec_root(f'ls -1 {part_path}')}"
    return removed


def marks_file_of(removed, stream):
    # The extension varies with the mark type, so pick the removed file by stream rather than
    # hardcoding one: `<column>` for a wide part, `data` for a compact part's single marks file.
    matches = [name for name in removed if name.startswith(f"{stream}.")]
    assert len(matches) == 1, f"expected one {stream}.* marks file, got {removed}"
    return matches[0]


def listed_in(text, label):
    start = text.index(f"{label}: [") + len(f"{label}: [")
    return [entry.strip() for entry in text[start : text.index("]", start)].split(",")]


def assert_names_the_part_state(text, part_name, removed, stream):
    # The point of the diagnostic is the payload, so assert the payload and not just the exception
    # name: the part, the specific missing marks file, its checksums status, and the contents of
    # both listings. Generic phrases alone would still pass if any of these were dropped, and a
    # listing's label alone would still pass if its contents were wrong.
    #
    # A message read back out of `system.text_log` arrives escaped, so undo the quote escaping to
    # let one set of assertions serve both that and a client-side error string.
    text = text.replace("\\'", "'")
    marks_file = marks_file_of(removed, stream)

    # The marks file is asserted with its `Marks file '...'` prefix: the bare name also appears in
    # the checksums listing below, where its presence says nothing about which file was reported.
    assert f"Marks file '{marks_file}'" in text, text
    assert f"in part '{part_name}'" in text, text
    assert "The file is listed in the part's checksums" in text, text
    assert "Part columns: [" in text, text

    # The two listings are the diagnostic's answer to "what does the part think it has, and what
    # does it actually have", so the missing marks file has to appear in exactly one of them. The
    # surviving data file pins the disk listing to the part's real contents rather than to an
    # empty or unrelated list.
    assert marks_file in listed_in(text, "Checksums files"), text
    on_disk = listed_in(text, "Files on disk")
    assert marks_file not in on_disk, text
    assert f"{stream}.bin" in on_disk, text


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


def logged_diagnostic(table):
    # `reason` in `system.detached_parts` is `broken-on-start` whether the load failed with the
    # typed diagnostic or with the opaque `std::filesystem` error, so it cannot tell the two apart.
    # A load path reports through the log rather than to a client, so the log is where the two are
    # separable. The logger is `default.<table> (<uuid>)` under Atomic and `default.<table>` under
    # Ordinary; the pattern matches both.
    # The load failure is logged twice: once as the diagnostic itself and once wrapped in the
    # broken-part report. Which one carries the full payload is an implementation detail, so join
    # every matching row and assert against that.
    node.query("SYSTEM FLUSH LOGS text_log")
    return node.query(f"""SELECT arrayStringConcat(groupArray(message), ' | ')
            FROM system.text_log
            WHERE event_time > now() - INTERVAL 5 MINUTE
              AND logger_name LIKE '%default.{table}%'
              AND message LIKE '%does not exist on disk in part%'""")


def test_query_read_path(started_cluster):
    # `MergeTreeMarksLoader::loadMarksImpl`: the marks are loaded lazily while a query reads the
    # column, so the error reaches the client.
    table = "t_missing_marks"
    try:
        create_and_fill(table, WIDE_SETTINGS)
        part_name, part_path = active_part(table)
        removed = remove_all_marks_files(part_path)
        node.query("SYSTEM DROP MARK CACHE")

        # The query reads only b, so the loader is asked for b's marks.
        error = node.query_and_get_error(f"SELECT sum(b) FROM {table}")
        assert "NO_FILE_IN_DATA_PART" in error, error
        assert_names_the_part_state(error, part_name, removed, "b")
    finally:
        drop_table(table)


def test_wide_index_granularity_load_path(started_cluster):
    # `MergeTreeDataPartWide::loadIndexGranularity`: index granularity is read from the first
    # column's marks file when the part is loaded, before any query reaches the marks loader.
    table = "t_missing_granularity_marks"
    try:
        create_and_fill(table, WIDE_SETTINGS)
        part_name, part_path = active_part(table)

        # Detaching unloads the part, so ATTACH below re-reads index granularity from disk.
        node.query(f"DETACH TABLE {table}")
        removed = remove_all_marks_files(part_path)
        node.query(f"ATTACH TABLE {table}", settings={"send_logs_level": "none"})

        assert (
            node.query(
                f"SELECT reason FROM system.detached_parts WHERE database = 'default' AND table = '{table}'"
            )
            == "broken-on-start\n"
        )
        # a is the first column, so it is the one this path resolves its marks file from.
        assert_names_the_part_state(logged_diagnostic(table), part_name, removed, "a")
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
        part_name, part_path = active_part(table)

        node.query(f"DETACH TABLE {table}")
        removed = remove_all_marks_files(part_path)
        node.query(f"ATTACH TABLE {table}", settings={"send_logs_level": "none"})

        assert (
            node.query(
                f"SELECT reason FROM system.detached_parts WHERE database = 'default' AND table = '{table}'"
            )
            == "broken-on-start\n"
        )
        # Asserting on the "data" marks file is what proves the compact path produced this
        # diagnostic: the wide path would have named a per-column marks file instead.
        assert_names_the_part_state(
            logged_diagnostic(table), part_name, removed, "data"
        )
    finally:
        drop_table(table)
