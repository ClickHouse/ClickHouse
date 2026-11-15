"""Mutations over parts whose on-disk files were tampered with.

Old-version emulation (removed `columns_substreams.txt`) and corrupted/legacy
skip-index files in active parts. The file surgery goes through the table data
directory inside the container, exactly like the original stateless tests did
on the local filesystem.
"""

import shlex

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def container_bash(cmd):
    return node.exec_in_container(["bash", "-c", cmd], privileged=True, user="root")


def path_exists(path, flag="-e"):
    return container_bash(f"test {flag} {path} && echo 1 || echo 0").strip() == "1"


def glob_exists(pattern):
    return (
        container_bash(f"ls {pattern} >/dev/null 2>&1 && echo 1 || echo 0").strip()
        == "1"
    )


def table_data_path(table):
    return node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = 'default' AND table = '{table}'"
    ).strip()


def active_part_path(table, order_by_name=False):
    order = "ORDER BY name " if order_by_name else ""
    return node.query(
        f"SELECT path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active {order}LIMIT 1"
    ).strip()


def test_dynamic_mutation_old_part_no_substreams_file(started_cluster):
    # Converted from stateless test 04401_dynamic_mutation_old_part_no_substreams_file.sh.
    #
    # Regression for a Wide part that has a Dynamic column but no columns_substreams.txt, as written by
    # servers from before that file existed (Dynamic became production-ready in 25.3, the file was added
    # to Wide parts in 25.8). For such a part the mutation stream-accounting cannot enumerate the
    # data-dependent substreams of the Dynamic column (variant_discr, ...) without a deserialization
    # state, so a partial mutation could leave one of those streams neither rewritten nor hardlinked.
    # The mutation must instead rewrite the whole part. We simulate the old part by deleting
    # columns_substreams.txt and reloading the table, then run a partial mutation that does NOT touch the
    # Dynamic column and validate the resulting part with CHECK TABLE.
    # See https://github.com/ClickHouse/ClickHouse/issues/107561
    node.query("DROP TABLE IF EXISTS t_dyn_old_part")

    node.query("""
        CREATE TABLE t_dyn_old_part (id UInt64, s UInt64, y Dynamic(max_types=3))
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0
        """)

    node.query(
        "INSERT INTO t_dyn_old_part SELECT number, number, number::Int64 FROM numbers(1000)"
    )
    node.query(
        "INSERT INTO t_dyn_old_part SELECT number, number, 's' || number FROM numbers(1000)"
    )
    node.query("OPTIMIZE TABLE t_dyn_old_part FINAL")

    data_path = node.query(
        "SELECT path FROM system.parts WHERE database = 'default' AND table = 't_dyn_old_part' AND active"
    ).strip()

    # columns_substreams.txt present before
    assert path_exists(f"{data_path}columns_substreams.txt", flag="-f")

    # Detach the table, delete columns_substreams.txt to simulate a part written before the file existed,
    # then reload it from disk.
    node.query("DETACH TABLE t_dyn_old_part")
    container_bash(f"rm -f {data_path}columns_substreams.txt")
    node.query("ATTACH TABLE t_dyn_old_part")

    # columns_substreams.txt not present after delete+attach
    assert not path_exists(f"{data_path}columns_substreams.txt", flag="-f")

    # Partial mutation that does NOT touch the Dynamic column. Because the source part has a Dynamic
    # column with no recorded substreams, the whole part must be rewritten.
    node.query(
        "ALTER TABLE t_dyn_old_part UPDATE s = s + 1 WHERE id % 2 = 0 SETTINGS mutations_sync = 2"
    )

    # Data after mutation
    assert (
        node.query(
            "SELECT count(), countIf(y IS NOT NULL), countIf(s = id + (id % 2 = 0)) FROM t_dyn_old_part"
        )
        == "2000\t2000\t2000\n"
    )
    assert (
        node.query(
            "SELECT dynamicType(y) AS t, count() FROM t_dyn_old_part GROUP BY t ORDER BY t"
        )
        == "Int64\t1000\nString\t1000\n"
    )

    assert (
        node.query(
            "CHECK TABLE t_dyn_old_part SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )

    # The rewritten part is in the modern format again: columns_substreams.txt exists and records the
    # Dynamic column's variant_discr substream.
    new_data_path = node.query(
        "SELECT path FROM system.parts WHERE database = 'default' AND table = 't_dyn_old_part' AND active"
    ).strip()
    assert (
        container_bash(
            f"grep -q variant_discr {new_data_path}columns_substreams.txt 2>/dev/null && echo 1 || echo 0"
        ).strip()
        == "1"
    )

    node.query("DROP TABLE t_dyn_old_part SYNC")


def test_mutation_old_part_basic_map_partial(started_cluster):
    # Converted from stateless test 04412_mutation_old_part_basic_map_partial.sh.
    #
    # Companion to 04401: a Wide part with a plain Map column but no columns_substreams.txt (as written by
    # servers from before that file existed) must NOT be force-rewritten by a partial mutation of an
    # unrelated column. A basic Map serialization enumerates all of its physical streams without a
    # deserialization state (unlike Dynamic/JSON, whose data-dependent substreams require the state), so a
    # partial mutation can account for every stream and stay on the cheap partial path. We simulate the old
    # part by deleting columns_substreams.txt and reloading the table, run a partial mutation that does NOT
    # touch the Map column, validate with CHECK TABLE, and assert the rewritten part did NOT regenerate
    # columns_substreams.txt (which would have meant a needless full rewrite of all the Map data).
    # See https://github.com/ClickHouse/ClickHouse/issues/107561
    node.query("DROP TABLE IF EXISTS t_map_old_part")

    node.query("""
        CREATE TABLE t_map_old_part (id UInt64, s UInt64, m Map(String, String))
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0
        """)

    node.query(
        "INSERT INTO t_map_old_part SELECT number, number, map('id', toString(number), 'k', toString(number * 2)) FROM numbers(1000)"
    )
    node.query("OPTIMIZE TABLE t_map_old_part FINAL")

    data_path = node.query(
        "SELECT path FROM system.parts WHERE database = 'default' AND table = 't_map_old_part' AND active"
    ).strip()

    # columns_substreams.txt present before
    assert path_exists(f"{data_path}columns_substreams.txt", flag="-f")

    # Detach the table, delete columns_substreams.txt to simulate a part written before the file existed,
    # then reload it from disk.
    node.query("DETACH TABLE t_map_old_part")
    container_bash(f"rm -f {data_path}columns_substreams.txt")
    node.query("ATTACH TABLE t_map_old_part")

    # columns_substreams.txt not present after delete+attach
    assert not path_exists(f"{data_path}columns_substreams.txt", flag="-f")

    # Partial mutation that does NOT touch the Map column. The Map's streams are fully enumerable without a
    # deserialization state, so there is no correctness need to rewrite the whole part: it must stay on the
    # partial path.
    node.query(
        "ALTER TABLE t_map_old_part UPDATE s = s + 1 WHERE id % 2 = 0 SETTINGS mutations_sync = 2"
    )

    # Data after mutation
    assert (
        node.query(
            "SELECT count(), countIf(s = id + (id % 2 = 0)), countIf(m['id'] = toString(id) AND m['k'] = toString(id * 2)) FROM t_map_old_part"
        )
        == "1000\t1000\t1000\n"
    )

    assert (
        node.query(
            "CHECK TABLE t_map_old_part SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )

    # A partial mutation of a part with no columns_substreams.txt does not write one (it is only filled from
    # a non-empty source). So the absence of the file proves the part stayed on the partial path; if the Map
    # column had wrongly forced a full rewrite, the part would have regained columns_substreams.txt.
    new_data_path = node.query(
        "SELECT path FROM system.parts WHERE database = 'default' AND table = 't_map_old_part' AND active"
    ).strip()
    assert not path_exists(f"{new_data_path}columns_substreams.txt", flag="-f")

    node.query("DROP TABLE t_map_old_part SYNC")


def test_mutate_repair_corrupted_missing_idx_checksums(started_cluster):
    # Converted from stateless test 04426_mutate_repair_corrupted_missing_idx_checksums.sh.
    #
    # Regression for the migration case flagged on PR #109616 (issue #109595).
    # The pre-#109595 releases (26.3+) rewrote a mutated `Wide` part's `checksums.txt`
    # WITHOUT the hardlinked skip-index files, producing a corrupted part shape:
    # skp_idx_<name>.* files present on disk, but no per-file entries in
    # `checksums.txt`. Such an index is already dead (reads probe checksums and see
    # nothing, so it never prunes; `CHECK TABLE` fails with
    # `UNEXPECTED_FILE_IN_DATA_PART`). A later full-part-rewrite mutation used to take
    # the preserve path (`getAllSubstreamsInPart` over checksums returns no substreams)
    # and drop the orphan files without repairing the part, losing the index
    # forever. The fix detects the shape (index present on disk but unresolvable
    # from checksums) and forces a recalculate so the writer rebuilds the index.
    #
    # The original stateless test pins `packed_skip_index_max_bytes` = 0 in the CREATE because it
    # depends on the standalone (non-packed) index file that the surgery injects, and depends on a
    # fixed granule count (`no-random-merge-tree-settings`).
    node.query("DROP TABLE IF EXISTS t_corrupt_minmax SYNC")

    # v = k is monotone, so a minmax index over v prunes a point query to a single
    # granule. `index_granularity` = 100 over 2000 rows gives 20 granules. m is a
    # MATERIALIZED column so that `DROP COLUMN` m forces a full-part rewrite
    # (`MutateAllPartColumnsTask`).
    node.query("""
        CREATE TABLE t_corrupt_minmax
        (
            k UInt64,
            v UInt64,
            m Map(String, UInt64) MATERIALIZED map('a', k),
            INDEX mm_v v TYPE minmax GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """)

    node.query(
        "INSERT INTO t_corrupt_minmax (k, v) SELECT number, number FROM numbers(2000)"
    )

    data_path = table_data_path("t_corrupt_minmax")
    active_part = active_part_path("t_corrupt_minmax")

    # Save the freshly written index files, then drop and re-declare the index so
    # the active part has NO skp_idx entries in `checksums.txt`, and re-inject the
    # saved files. This reproduces the released-bug shape without depending on an
    # old binary: index files on disk, missing from `checksums.txt`.
    container_bash(f"cp {active_part}skp_idx_mm_v.idx2 {data_path}/saved_mm_v.idx2")
    container_bash(f"cp {active_part}skp_idx_mm_v.cmrk2 {data_path}/saved_mm_v.cmrk2")

    node.query(
        "ALTER TABLE t_corrupt_minmax DROP INDEX mm_v SETTINGS mutations_sync = 2"
    )
    node.query(
        "ALTER TABLE t_corrupt_minmax ADD INDEX mm_v v TYPE minmax GRANULARITY 1"
    )

    corrupt_part = active_part_path("t_corrupt_minmax")
    container_bash(f"cp {data_path}/saved_mm_v.idx2 {corrupt_part}skp_idx_mm_v.idx2")
    container_bash(f"cp {data_path}/saved_mm_v.cmrk2 {corrupt_part}skp_idx_mm_v.cmrk2")

    # The corrupted index is already dead: files are on disk but not in checksums,
    # so it does not prune and `CHECK TABLE` fails.
    assert (
        node.query(
            "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_corrupt_minmax WHERE v = 1042) WHERE explain ILIKE '%Granules: 1/20%'"
        )
        == "0\n"
    )

    # Full-part rewrite (`MutateAllPartColumnsTask` via `DROP COLUMN` of a MATERIALIZED
    # column). Before the fix the preserve path found no substreams in checksums and
    # dropped the orphan files, permanently losing the index. The fix forces a
    # recalculate so the writer rebuilds the index from column data.
    node.query("ALTER TABLE t_corrupt_minmax DROP COLUMN m SETTINGS mutations_sync = 2")

    new_part = active_part_path("t_corrupt_minmax")
    assert glob_exists(f"{new_part}skp_idx_mm_v.idx2")

    # The repaired index must prune to one granule, `CHECK TABLE` must pass, and the
    # on-disk size accounting must include the rebuilt index.
    assert (
        node.query(
            "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_corrupt_minmax WHERE v = 1042) WHERE explain ILIKE '%Granules: 1/20%'"
        )
        == "1\n"
    )
    assert (
        node.query(
            "CHECK TABLE t_corrupt_minmax SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT secondary_indices_compressed_bytes > 0 FROM system.parts WHERE database = 'default' AND table = 't_corrupt_minmax' AND active LIMIT 1"
        )
        == "1\n"
    )
    assert node.query("SELECT count() FROM t_corrupt_minmax WHERE v = 1042") == "1\n"

    node.query("DROP TABLE t_corrupt_minmax SYNC")


def test_mutate_some_columns_drop_index_corrupted_idx(started_cluster):
    # Converted from stateless test 04427_mutate_some_columns_drop_index_corrupted_idx.sh.
    #
    # Regression for the migration case flagged on PR #109616 (issue #109595).
    # 04426 covers the FULL-part-rewrite path (`MutateAllPartColumnsTask`), which
    # repairs a part corrupted by the released #109595 bug (skp_idx_<name>.* on
    # disk, no per-file entries in `checksums.txt`) by recomputing the index. This
    # test covers the two remaining paths that a corrupted part can go through:
    #   A) a some-columns mutation (`ALTER UPDATE` of a non-indexed column), and
    #   B) `DROP INDEX`.
    # Their bookkeeping resolved index files only through `checksums.txt`, so the
    # orphan standalone files were hardlinked into the new part unchanged and
    # `CHECK TABLE` kept failing. The fix drops the dead orphan files on both paths,
    # leaving the part consistent (the index is simply absent from this part -- a
    # later `MATERIALIZE INDEX` repopulates it; a full rewrite does not, because once
    # the orphans are gone the index is no longer present on disk and so is not
    # selected for recalculation).
    #
    # The original stateless test pins `packed_skip_index_max_bytes` = 0 in both CREATEs because it
    # depends on the standalone (non-packed) index file that the surgery injects, and depends on a
    # fixed granule count (`no-random-merge-tree-settings`).

    # Fabricate a part in the released-bug shape: skp_idx_mm_v.* on disk but absent
    # from `checksums.txt`. Save the freshly written index files, DROP+re-ADD the
    # index so the active part has no skp_idx entries in checksums, then re-inject.
    def make_corrupted_part(tbl):
        node.query(f"DROP TABLE IF EXISTS {tbl} SYNC")
        node.query(f"""
            CREATE TABLE {tbl}
            (
                k UInt64,
                v UInt64,
                w UInt64,
                INDEX mm_v v TYPE minmax GRANULARITY 1
            )
            ENGINE = MergeTree ORDER BY k
            SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                     index_granularity = 100, replace_long_file_name_to_hash = 0,
                     packed_skip_index_max_bytes = 0,
                     columns_and_secondary_indices_sizes_lazy_calculation = 0
            """)

        node.query(
            f"INSERT INTO {tbl} (k, v, w) SELECT number, number, number FROM numbers(2000)"
        )

        data_path = table_data_path(tbl)
        active = active_part_path(tbl)

        container_bash(f"cp {active}skp_idx_mm_v.idx2 {data_path}/saved_{tbl}.idx2")
        container_bash(f"cp {active}skp_idx_mm_v.cmrk2 {data_path}/saved_{tbl}.cmrk2")

        node.query(f"ALTER TABLE {tbl} DROP INDEX mm_v SETTINGS mutations_sync = 2")
        node.query(f"ALTER TABLE {tbl} ADD INDEX mm_v v TYPE minmax GRANULARITY 1")

        corrupt = active_part_path(tbl)
        container_bash(f"cp {data_path}/saved_{tbl}.idx2 {corrupt}skp_idx_mm_v.idx2")
        container_bash(f"cp {data_path}/saved_{tbl}.cmrk2 {corrupt}skp_idx_mm_v.cmrk2")

    def orphan_on_disk(tbl):
        part = active_part_path(tbl)
        return glob_exists(f"{part}skp_idx_mm_v.*")

    # --- Path A: some-columns mutation (`ALTER UPDATE` of the non-indexed column w) ---
    make_corrupted_part("t_mm_some")
    assert orphan_on_disk("t_mm_some")
    node.query(
        "ALTER TABLE t_mm_some UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2"
    )
    assert not orphan_on_disk("t_mm_some")
    assert (
        node.query("CHECK TABLE t_mm_some SETTINGS check_query_single_value_result = 1")
        == "1\n"
    )
    assert node.query("SELECT count() FROM t_mm_some WHERE v = 1042") == "1\n"
    node.query("DROP TABLE t_mm_some SYNC")

    # --- Path B: `DROP INDEX` on a corrupted part ---
    make_corrupted_part("t_mm_drop")
    assert orphan_on_disk("t_mm_drop")
    node.query("ALTER TABLE t_mm_drop DROP INDEX mm_v SETTINGS mutations_sync = 2")
    assert not orphan_on_disk("t_mm_drop")
    assert (
        node.query("CHECK TABLE t_mm_drop SETTINGS check_query_single_value_result = 1")
        == "1\n"
    )
    node.query("DROP TABLE t_mm_drop SYNC")

    # --- Path C (no regression): a healthy part keeps its index through a some-columns mutation ---
    # `packed_skip_index_max_bytes` = 0 keeps this control on the standalone
    # (non-packed) preserve path that paths A and B exercise; without it the control
    # would assert over packed-archive preservation instead (covered by 04403).
    node.query("DROP TABLE IF EXISTS t_mm_ok SYNC")
    node.query("""
        CREATE TABLE t_mm_ok (k UInt64, v UInt64, w UInt64, INDEX mm_v v TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 packed_skip_index_max_bytes = 0
        """)
    node.query(
        "INSERT INTO t_mm_ok (k, v, w) SELECT number, number, number FROM numbers(2000)"
    )
    node.query(
        "ALTER TABLE t_mm_ok UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2"
    )
    assert (
        node.query(
            "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mm_ok WHERE v = 1042) WHERE explain ILIKE '%Granules: 1/20%'"
        )
        == "1\n"
    )
    assert (
        node.query("CHECK TABLE t_mm_ok SETTINGS check_query_single_value_result = 1")
        == "1\n"
    )
    node.query("DROP TABLE t_mm_ok SYNC")


def test_mutate_corrupted_text_index_multistream(started_cluster):
    # Converted from stateless test 04428_mutate_corrupted_text_index_multistream.sh.
    #
    # Regression for the multi-stream text-index case flagged on PR #109616 (issue #109595).
    # 04427 covers the corrupted-orphan repair (skp_idx_<name>.* on disk, no per-file entries
    # in `checksums.txt`) for a single-stream minmax index on the some-columns mutation and
    # `DROP INDEX` paths. A text index owns several substreams -- the base .idx plus .dct, .pst
    # and, with positions enabled, .pos -- each with its own data file and mark. The orphan
    # scan and the `DROP INDEX` rename fallback previously enumerated only the base .idx/.idx2
    # plus one mark, so the .dct/.pst/.pos side streams of a corrupted text part were hardlinked
    # into the new part unchanged and `CHECK TABLE` kept failing with `UNEXPECTED_FILE_IN_DATA_PART`.
    # Paths D and E cover the same corruption with the base .idx pair also gone, where a presence
    # check limited to the base extensions reports the part as index-free and no repair runs at all.

    # Fabricate a part in the released-bug shape: skp_idx_txt.* on disk but absent from
    # `checksums.txt`. Save the freshly written index files, DROP+re-ADD the index so the active
    # part has no skp_idx entries in checksums, then re-inject the files on disk.
    # Returns the poisoned part's path, so callers never re-query it.
    def make_corrupted_part(tbl, mode="all"):
        # m mirrors 04426: a MATERIALIZED Map column, so `DROP COLUMN m` reaches
        # `MutateAllPartColumnsTask`. A scalar MATERIALIZED column is not enough -- dropping one
        # still takes the some-columns path (verified via `MutationAllPartColumns` in `system.part_log`).
        #
        # Granule-selective like t_txt_ok below: the phrase sits only in the first 100 rows, i.e. in one
        # granule out of 20, so a `Granules: 1/20` assertion can tell a working positional index from
        # a silently declining one. A pure modulo fixture puts the phrase in every granule and makes
        # any pruning assertion vacuous. The modulo tokens stay for the `hasToken` counts.
        node.query(f"DROP TABLE IF EXISTS {tbl} SYNC")
        node.query(f"""
            CREATE TABLE {tbl}
            (
                k UInt64,
                s String,
                w UInt64,
                m Map(String, UInt64) MATERIALIZED map('a', k),
                INDEX txt(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1
            )
            ENGINE = MergeTree ORDER BY k
            SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                     index_granularity = 100, replace_long_file_name_to_hash = 0,
                     columns_and_secondary_indices_sizes_lazy_calculation = 0,
                     allow_experimental_text_index_phrase_search = 1
            """)
        node.query(
            f"INSERT INTO {tbl} (k, s, w) SELECT number, if(number < 100, 'needle alpha beta', concat('hello', number % 50, ' world', number % 50)), number FROM numbers(2000)"
        )
        data_path = table_data_path(tbl)
        active = active_part_path(tbl)

        container_bash(f"rm -rf {data_path}/saved_{tbl}")
        container_bash(f"mkdir -p {data_path}/saved_{tbl}")
        container_bash(f"cp {active}skp_idx_txt.* {data_path}/saved_{tbl}/")

        node.query(f"ALTER TABLE {tbl} DROP INDEX txt SETTINGS mutations_sync = 2")
        node.query(
            f"ALTER TABLE {tbl} ADD INDEX txt(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1"
        )
        corrupt = active_part_path(tbl)

        if mode == "side_streams_only":
            # Base .idx pair deliberately omitted: a part poisoned this way is still corrupted, but
            # a presence check that probes only the base .idx/.idx2 reports it as index-free.
            container_bash(
                f"cp {data_path}/saved_{tbl}/skp_idx_txt.dct.* {data_path}/saved_{tbl}/skp_idx_txt.pst.* "
                f"{data_path}/saved_{tbl}/skp_idx_txt.pos.* {corrupt}"
            )
        else:
            container_bash(f"cp {data_path}/saved_{tbl}/skp_idx_txt.* {corrupt}")

        return corrupt

    # Both helpers take the part path, which every caller already holds from the statement that
    # produced the part.
    def orphan_on_disk(part):
        return glob_exists(f"{part}skp_idx_txt.*")

    # Count the fabricated files one by one instead of globbing. A glob over
    # skp_idx_txt.* stays green even if an entire substream silently stops being
    # written -- in particular the positional .pos pair, which only exists while
    # `support_phrase_search` is on -- and that would make the orphan-cleanup
    # assertions vacuous for exactly the substreams this test is about. Expect 8:
    # base, .dct, .pst and .pos, each with a data file and a mark file.
    def side_streams_on_disk(part):
        files = [
            "skp_idx_txt.idx",
            "skp_idx_txt.cmrk2",
            "skp_idx_txt.dct.idx",
            "skp_idx_txt.dct.cmrk2",
            "skp_idx_txt.pst.idx",
            "skp_idx_txt.pst.cmrk2",
            "skp_idx_txt.pos.idx",
            "skp_idx_txt.pos.cmrk2",
        ]
        cmd = (
            "n=0; for f in " + " ".join(files) + "; do "
            f"if [ -e {part}$f ]; then n=$((n + 1)); fi; done; echo $n"
        )
        return int(container_bash(cmd).strip())

    # --- Path A: some-columns mutation (`ALTER UPDATE` of the non-indexed column w) ---
    corrupt_part = make_corrupted_part("t_txt_some")
    assert orphan_on_disk(corrupt_part)
    assert side_streams_on_disk(corrupt_part) == 8
    node.query(
        "ALTER TABLE t_txt_some UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2"
    )
    new_part = active_part_path("t_txt_some")
    assert not orphan_on_disk(new_part)
    assert (
        node.query(
            "CHECK TABLE t_txt_some SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )
    assert (
        node.query("SELECT count() FROM t_txt_some WHERE hasToken(s, 'hello10')")
        == "38\n"
    )
    node.query("DROP TABLE t_txt_some SYNC")

    # --- Path B: `DROP INDEX` on a corrupted part ---
    corrupt_part = make_corrupted_part("t_txt_drop")
    assert orphan_on_disk(corrupt_part)
    assert side_streams_on_disk(corrupt_part) == 8
    node.query("ALTER TABLE t_txt_drop DROP INDEX txt SETTINGS mutations_sync = 2")
    new_part = active_part_path("t_txt_drop")
    assert not orphan_on_disk(new_part)
    assert (
        node.query(
            "CHECK TABLE t_txt_drop SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )
    node.query("DROP TABLE t_txt_drop SYNC")

    # --- Path C (no regression): a healthy text index survives a some-columns mutation ---
    # Granule-selective on purpose: the phrase occurs only in the first 100 rows, i.e.
    # in one granule out of 20, so the EXPLAIN assertion below can actually tell a
    # working positional index from a silently declining one. A modulo fixture would
    # put the phrase in every granule and make any pruning assertion vacuous.
    node.query("DROP TABLE IF EXISTS t_txt_ok SYNC")
    node.query("""
        CREATE TABLE t_txt_ok (k UInt64, s String, w UInt64, INDEX txt(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 allow_experimental_text_index_phrase_search = 1
        """)
    node.query(
        "INSERT INTO t_txt_ok (k, s, w) SELECT number, if(number < 100, 'needle alpha beta', concat('hello', number % 50, ' world', number % 50)), number FROM numbers(2000)"
    )
    node.query(
        "ALTER TABLE t_txt_ok UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2"
    )
    assert (
        node.query(
            "SELECT count() > 0 FROM system.parts WHERE database = 'default' AND table = 't_txt_ok' AND active AND secondary_indices_marks_bytes > 0"
        )
        == "1\n"
    )
    # Every substream, including the positional pair, must survive the mutation
    # individually -- an aggregate mark size stays positive even if .pos is lost.
    assert side_streams_on_disk(active_part_path("t_txt_ok")) == 8
    # `hasPhrase` reads the positional substream, so it fails if `.pos` was dropped, and the
    # index must still PRUNE for it, which a count alone cannot show: a declining index
    # would return the same 100 rows via a full scan.
    assert (
        node.query("CHECK TABLE t_txt_ok SETTINGS check_query_single_value_result = 1")
        == "1\n"
    )
    assert (
        node.query("SELECT count() FROM t_txt_ok WHERE hasToken(s, 'hello10')")
        == "38\n"
    )
    assert (
        node.query("SELECT count() FROM t_txt_ok WHERE hasPhrase(s, 'needle alpha')")
        == "100\n"
    )
    assert (
        node.query(
            "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_ok WHERE hasPhrase(s, 'needle alpha')) WHERE explain ILIKE '%Granules: 1/20%'"
        )
        == "1\n"
    )
    node.query("DROP TABLE t_txt_ok SYNC")

    # --- Paths D and E: the same corruption with the base .idx pair ALSO missing ---
    # Paths A-C keep the base skp_idx_txt.idx on disk, so a presence check that probes only the
    # base extensions still sees the index and both repair paths run. Drop that pair and only the
    # .dct/.pst/.pos side streams remain: the part is just as corrupted, but a base-only check
    # reports it as index-free, so nothing collects the orphans and the full rewrite hardlinks
    # them forward with no checksum entries. Expect 6 files (three substreams, data plus mark).
    corrupt_part = make_corrupted_part("t_txt_side", mode="side_streams_only")
    assert side_streams_on_disk(corrupt_part) == 6
    node.query(
        "ALTER TABLE t_txt_side UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2"
    )
    new_part = active_part_path("t_txt_side")
    assert not orphan_on_disk(new_part)
    assert (
        node.query(
            "CHECK TABLE t_txt_side SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )
    assert (
        node.query("SELECT count() FROM t_txt_side WHERE hasToken(s, 'hello10')")
        == "38\n"
    )
    node.query("DROP TABLE t_txt_side SYNC")

    # Path E is the full-rewrite arm of the same shape: dropping the MATERIALIZED Map column m
    # reaches `MutateAllPartColumnsTask`, which rebuilds the index from column data instead of
    # leaving it absent (04426 asserts the same repair for a single-stream minmax index). So here
    # the index files are expected to be BACK and checksummed, and to prune again -- unlike paths
    # A/B/D, where the orphans are removed and the index stays absent until `MATERIALIZE INDEX`.
    corrupt_part = make_corrupted_part("t_txt_side_full", mode="side_streams_only")
    assert side_streams_on_disk(corrupt_part) == 6
    node.query("ALTER TABLE t_txt_side_full DROP COLUMN m SETTINGS mutations_sync = 2")
    new_part = active_part_path("t_txt_side_full")
    # All 8 files, not just the 6 injected ones: a rebuild writes the base pair too.
    assert side_streams_on_disk(new_part) == 8
    # The rebuilt index must actually prune, which the file count alone cannot show.
    assert (
        node.query(
            "CHECK TABLE t_txt_side_full SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )
    assert (
        node.query("SELECT count() FROM t_txt_side_full WHERE hasToken(s, 'hello10')")
        == "38\n"
    )
    assert (
        node.query(
            "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_txt_side_full WHERE hasPhrase(s, 'needle alpha')) WHERE explain ILIKE '%Granules: 1/20%'"
        )
        == "1\n"
    )
    node.query("DROP TABLE t_txt_side_full SYNC")

    # --- Path F: a sibling index registers a file the corrupted text index also addresses ---
    # With `escape_index_filenames` = 0 an index NAME may equal a text substream name, so a minmax
    # index called `a.pst` and the `.pst` substream of a text index `a` both address
    # `skp_idx_a.pst.cmrk2`. Only the minmax writes it, and it is in `checksums.txt`. Resolving
    # orphan candidates against storage alone claims that file for the corrupted text index, so it
    # is skipped while its checksum entry survives -> `CHECK TABLE` fails with NO_FILE_IN_DATA_PART.
    # The registered owner here is dropped by the very mutation under test, so it cannot be found
    # in the (already post-drop) metadata; only checksum membership identifies the file.
    # The sibling name is the only variable: `b` is the control, `a.pst` collides.
    def run_sibling_owns_file_case(label, sib, expected_sibling_owns_contested_name):
        tbl = f"t_txt_own_{label}"

        node.query(f"DROP TABLE IF EXISTS {tbl} SYNC")
        node.query(f"""
            CREATE TABLE {tbl}
            (
                k UInt64,
                s String,
                w UInt64,
                INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1
            )
            ENGINE = MergeTree ORDER BY k
            SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                     index_granularity = 100, replace_long_file_name_to_hash = 0,
                     escape_index_filenames = 0, packed_skip_index_max_bytes = 0,
                     columns_and_secondary_indices_sizes_lazy_calculation = 0,
                     allow_experimental_text_index_phrase_search = 1
            """)
        node.query(
            f"INSERT INTO {tbl} (k, s, w) SELECT number, concat('hello', number % 50, ' world', number % 50), number FROM numbers(500)"
        )
        dp = table_data_path(tbl)
        act = active_part_path(tbl)
        container_bash(f"rm -rf {dp}/saved_{tbl}")
        container_bash(f"mkdir -p {dp}/saved_{tbl}")
        container_bash(f"cp {act}skp_idx_a.* {dp}/saved_{tbl}/")

        # Corrupt the text index while NO sibling exists, so nothing inherits a checksum entry for
        # its files, then add the healthy sibling. Order matters: adding the sibling first would let
        # it register a name the text index also addresses before the entries are stripped.
        # The sibling must be materialized and registered, else its file is not in `checksums.txt`,
        # the collision never arises, and the final `CHECK TABLE` passes for the wrong reason.
        node.query(f"ALTER TABLE {tbl} DROP INDEX a SETTINGS mutations_sync = 2")
        node.query(
            f"ALTER TABLE {tbl} ADD INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1"
        )
        node.query(f"ALTER TABLE {tbl} ADD INDEX `{sib}` w TYPE minmax GRANULARITY 1")
        node.query(
            f"ALTER TABLE {tbl} MATERIALIZE INDEX `{sib}` SETTINGS mutations_sync = 2"
        )
        cor = active_part_path(tbl)

        # Measured BEFORE reinjection, so it discriminates: only the colliding name makes the sibling
        # write the contested `skp_idx_a.pst.cmrk2` (0 for the control, 1 for the collision). After
        # reinjection the text fixture supplies that name in either case, so the same check there
        # would read 1 for both arms and prove nothing.
        assert (
            path_exists(f"{cor}skp_idx_a.pst.cmrk2")
            == expected_sibling_owns_contested_name
        ), label
        assert (
            node.query(
                f"SELECT count() FROM system.data_skipping_indices WHERE database = 'default' AND table = '{tbl}' AND name = '{sib}' AND marks_bytes > 0"
            )
            == "1\n"
        ), label
        assert (
            node.query(
                f"SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM {tbl} WHERE w = 42) WHERE explain ILIKE '%Granules: 1/5%'"
            )
            == "1\n"
        ), label

        # Re-inject the text index's files, never overwriting one the healthy sibling wrote.
        container_bash(
            f"for f in {dp}/saved_{tbl}/skp_idx_a.*; do "
            f'bn=$(basename "$f"); '
            f'if [ -e "{cor}$bn" ]; then continue; fi; '
            f'cp "$f" "{cor}"; '
            f"done"
        )

        node.query(f"ALTER TABLE {tbl} DROP INDEX `{sib}` SETTINGS mutations_sync = 2")
        new_part = active_part_path(tbl)

        # The corrupted text index's own orphans must still be cleaned up, so a green `CHECK TABLE`
        # cannot come from the repair silently doing nothing. Its base pair is never contested.
        assert not (
            path_exists(f"{new_part}skp_idx_a.idx")
            or path_exists(f"{new_part}skp_idx_a.dct.idx")
        ), label
        assert (
            node.query(
                f"CHECK TABLE {tbl} SETTINGS check_query_single_value_result = 1"
            )
            == "1\n"
        ), label
        node.query(f"DROP TABLE {tbl} SYNC")

    run_sibling_owns_file_case(
        "control", "b", expected_sibling_owns_contested_name=False
    )
    run_sibling_owns_file_case(
        "collide", "a.pst", expected_sibling_owns_contested_name=True
    )


def test_mutate_corrupted_index_sibling_owns_file(started_cluster):
    # Converted from stateless test 04431_mutate_corrupted_index_sibling_owns_file.sh.
    #
    # Regression for the inverse of the collision covered by 04429: a sibling's file must not make a
    # CORRUPTED index look healthy.
    #
    # `getAllSubstreamsInPart` probes speculative extensions - minmax tries its legacy `.idx` for a
    # `.idx2` substream - so with `escape_index_filenames` = 0, where the stream name is the index name
    # verbatim, that probe lands on a sibling's file: a corrupted minmax index named `a.pos` reaches the
    # checksummed `skp_idx_a.pos.idx` of text index `a`, which declares its `.pos` substream with
    # extension `.idx`. Counting a sibling's file as evidence of health classified the corrupted index as
    # intact, so `MutateAllPartColumnsTask` did not rebuild it and the some-columns orphan scan
    # hardlinked its orphan files forward, leaving `CHECK TABLE` failing with
    # `UNEXPECTED_FILE_IN_DATA_PART` on both paths.
    #
    # The original stateless test pins `escape_index_filenames` and `packed_skip_index_max_bytes`
    # (`no-random-merge-tree-settings`), which are exactly the settings the collision depends on.

    # Fabricate a part where ONLY the minmax index `a.pos` is corrupted: its file is on disk but has no
    # per-file entries in `checksums.txt`. The text index `a` stays fully healthy and checksummed, so its
    # `skp_idx_a.pos.idx` is the sibling file the corrupted index's legacy probe reaches.
    #
    # `packed_skip_index_max_bytes` = 0 is mandatory: the classification short-circuits to "resolvable"
    # for any index living in `skp_idx.packed`, which would make every assertion below vacuous.
    # `support_phrase_search` = 1 plus `allow_experimental_text_index_phrase_search` = 1 are mandatory
    # too - without them the text index declares no `.pos` substream and there is no collision at all.
    def make_corrupted_part(tbl):
        node.query(f"DROP TABLE IF EXISTS {tbl} SYNC")
        node.query(f"""
            CREATE TABLE {tbl}
            (
                k UInt64,
                s String,
                w UInt64,
                u UInt64,
                m Map(String, UInt64) MATERIALIZED map('a', k),
                INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
                INDEX `a.pos` w TYPE minmax GRANULARITY 1
            )
            ENGINE = MergeTree ORDER BY k
            SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                     index_granularity = 100, replace_long_file_name_to_hash = 0,
                     escape_index_filenames = 0, packed_skip_index_max_bytes = 0,
                     columns_and_secondary_indices_sizes_lazy_calculation = 0,
                     allow_experimental_text_index_phrase_search = 1
            """)

        node.query(
            f"INSERT INTO {tbl} (k, s, w, u) SELECT number, concat('hello', number % 50, ' world', number % 50), number, number FROM numbers(500)"
        )
        node.query(f"OPTIMIZE TABLE {tbl} FINAL")

        data_path = table_data_path(tbl)
        active = active_part_path(tbl, order_by_name=True)

        container_bash(f"rm -rf {data_path}/saved_{tbl}")
        container_bash(f"mkdir -p {data_path}/saved_{tbl}")
        # ONLY the minmax index's own `.idx2` payload. Its mark file `skp_idx_a.pos.cmrk2` is the same
        # filename the text index's positional substream writes, so copying it back would overwrite the
        # healthy sibling's mark and corrupt the very index this test asserts stays intact.
        container_bash(f"cp {active}skp_idx_a.pos.idx2 {data_path}/saved_{tbl}/")

        # DROP + re-ADD makes the active part carry no checksums entries for `a.pos`, then the saved
        # files are re-injected on disk. Re-ADD without `MATERIALIZE INDEX` leaves it unmaterialized,
        # which is the released-bug shape.
        node.query(f"ALTER TABLE {tbl} DROP INDEX `a.pos` SETTINGS mutations_sync = 2")
        node.query(f"ALTER TABLE {tbl} ADD INDEX `a.pos` w TYPE minmax GRANULARITY 1")

        corrupt = active_part_path(tbl, order_by_name=True)
        container_bash(f"cp {data_path}/saved_{tbl}/skp_idx_a.pos.idx2 {corrupt}")

    def orphan_on_disk(tbl):
        part = active_part_path(tbl, order_by_name=True)
        return path_exists(f"{part}skp_idx_a.pos.idx2")

    # Enumerate the text index's substreams one by one rather than globbing skp_idx_a.*: a glob stays
    # green even if an entire substream stops being written.
    #
    # The positional pair is deliberately EXCLUDED. With `escape_index_filenames` = 0 the minmax index
    # `a.pos` and the text index's own `.pos` substream want the same filenames, so this table already
    # loses `skp_idx_a.pos.idx` on any rewrite, before this test's corruption is introduced. That
    # write-time collision is a separate pre-existing issue (see 04429), and asserting its survival here
    # would encode existing breakage as expected behaviour. Expect 6: base, `.dct` and `.pst`, each with
    # a data file and a mark file.
    def text_streams_on_disk(tbl):
        part = active_part_path(tbl, order_by_name=True)
        files = [
            "skp_idx_a.idx",
            "skp_idx_a.cmrk2",
            "skp_idx_a.dct.idx",
            "skp_idx_a.dct.cmrk2",
            "skp_idx_a.pst.idx",
            "skp_idx_a.pst.cmrk2",
        ]
        cmd = (
            "n=0; for f in " + " ".join(files) + "; do "
            f"if [ -e {part}$f ]; then n=$((n + 1)); fi; done; echo $n"
        )
        return int(container_bash(cmd).strip())

    # --- Path A: full-part rewrite (`DROP COLUMN` of the MATERIALIZED `Map` column m) ---
    # `m` is a MATERIALIZED `Map`, i.e. a column with dynamic subcolumns, so dropping it forces
    # `MutateAllPartColumnsTask` (same device as 04426). Verified by `MutationAllPartColumns`: an
    # ordinary `UInt64` column would be handled as a file rename and take the some-columns path
    # instead, leaving this site unexercised.
    #
    # The dropped column is deliberately NOT the indexed one. An `ALTER UPDATE w` would put the index
    # in `materialized_indices`, which forces a recalculate on its own and would make the assertion
    # vacuous - it would stay green even with the classification broken.
    make_corrupted_part("t_sib_full")
    assert orphan_on_disk("t_sib_full")
    assert text_streams_on_disk("t_sib_full") == 6
    node.query("ALTER TABLE t_sib_full DROP COLUMN m SETTINGS mutations_sync = 2")
    # The index must be REBUILT, so its file is present again - but this time as a checksummed member of
    # the new part rather than as the hardlinked-forward orphan. `CHECK TABLE` is what separates the two:
    # with the classification counting the sibling's file as evidence of health the index is not
    # rebuilt, the unchecksummed orphan is carried into the new part, and this returns 0.
    assert orphan_on_disk("t_sib_full")
    assert (
        node.query(
            "CHECK TABLE t_sib_full SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )
    # A rebuilt minmax index prunes; a missing or stale one cannot. `w` = `k` is monotone and
    # `index_granularity` = 100 over 500 rows gives 5 granules.
    assert (
        node.query(
            "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sib_full WHERE w = 42) WHERE explain ILIKE '%Granules: 1/5%'"
        )
        == "1\n"
    )
    assert text_streams_on_disk("t_sib_full") == 6
    assert (
        node.query(
            "SELECT count() FROM system.data_skipping_indices WHERE database = 'default' AND table = 't_sib_full'"
        )
        == "2\n"
    )
    assert node.query("SELECT count() FROM t_sib_full") == "500\n"
    node.query("DROP TABLE t_sib_full SYNC")

    # --- Path B: some-columns mutation (`ALTER UPDATE` of the non-indexed column u) ---
    # Only `u` is rewritten, so the orphan scan decides: the corrupted index's orphan file must be
    # RECORDED and left behind instead of being hardlinked into the new part.
    make_corrupted_part("t_sib_some")
    assert orphan_on_disk("t_sib_some")
    assert text_streams_on_disk("t_sib_some") == 6
    node.query(
        "ALTER TABLE t_sib_some UPDATE u = u + 1 WHERE 1 SETTINGS mutations_sync = 2"
    )
    assert not orphan_on_disk("t_sib_some")
    assert (
        node.query(
            "CHECK TABLE t_sib_some SETTINGS check_query_single_value_result = 1"
        )
        == "1\n"
    )
    # The surviving text index must still be REGISTERED with readable substream sizes, which a file
    # count alone cannot show: `system.data_skipping_indices` reports the index only if the part's
    # checksums attribute its data and mark files to it, so a hardlinked-forward orphan reads 0.
    #
    # This is deliberately not a query-level assertion. Any query that makes this table's text index
    # prune has to open `skp_idx_a.pos.cmrk2`, which the minmax index `a.pos` and the text `.pos`
    # substream both write under `escape_index_filenames` = 0, so it throws `CANNOT_READ_ALL_DATA`
    # already on a pristine part -- the same pre-existing write-time collision `text_streams_on_disk`
    # excludes. A `hasToken` count does not throw only because an `ngrams` tokenizer makes it skip the
    # index entirely, which is what would make such an assertion vacuous.
    assert (
        node.query(
            "SELECT count() FROM system.data_skipping_indices WHERE database = 'default' AND table = 't_sib_some' AND name = 'a' AND marks_bytes > 0 AND data_compressed_bytes > 0"
        )
        == "1\n"
    )
    assert text_streams_on_disk("t_sib_some") == 6
    assert node.query("SELECT count() FROM t_sib_some") == "500\n"
    node.query("DROP TABLE t_sib_some SYNC")


@pytest.mark.parametrize(
    "before_escape,after_escape,value,remove_substreams",
    [
        (0, 1, (1, 2), False),
        (1, 0, (3, 4), False),
        (0, 1, (5, 6), True),
        (1, 0, (7, 8), True),
    ],
)
def test_variant_escape_filename_rename_consistency(
    started_cluster, before_escape, after_escape, value, remove_substreams
):
    # Converted from stateless test 04507_variant_escape_filename_rename_consistency.sh.
    # Keep the part-file surgery in a controlled container: stateless tests must not modify
    # server data on disk because their server configuration is not controlled.
    table = f"t_variant_escape_{before_escape}_{after_escape}_{int(remove_substreams)}"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"""
        CREATE TABLE {table} (v Variant(Tuple(a UInt32, b UInt32)))
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0, escape_variant_subcolumn_filenames = {before_escape},
            replace_long_file_name_to_hash = 0
        """)
    node.query(
        f"INSERT INTO {table} SELECT tuple({value[0]}, {value[1]})::Tuple(a UInt32, b UInt32)"
    )

    data_path = active_part_path(table)
    assert path_exists(f"{data_path}columns_substreams.txt", flag="-f")
    if remove_substreams:
        node.query(f"DETACH TABLE {table}")
        container_bash(f"rm -f {shlex.quote(data_path + 'columns_substreams.txt')}")
        node.query(f"ATTACH TABLE {table}")
    else:
        before = container_bash(
            f"cat {shlex.quote(data_path + 'columns_substreams.txt')}"
        )
        assert "3 substreams for column `v`:" in before

    node.query(
        f"ALTER TABLE {table} MODIFY SETTING escape_variant_subcolumn_filenames = {after_escape}"
    )
    node.query(f"ALTER TABLE {table} RENAME COLUMN v TO w SETTINGS mutations_sync = 2")
    assert (
        node.query(f"CHECK TABLE {table} SETTINGS check_query_single_value_result = 1")
        == "1\n"
    )
    assert (
        node.query(
            f"SELECT w, w.`Tuple(a UInt32, b UInt32)`.a, w.`Tuple(a UInt32, b UInt32)`.b FROM {table}"
        )
        == f"({value[0]},{value[1]})\t{value[0]}\t{value[1]}\n"
    )

    renamed_path = active_part_path(table)
    files = container_bash(
        f"find {shlex.quote(renamed_path)} -maxdepth 1 -name 'w.*.bin' -printf '%f\\n'"
    )
    escaped = before_escape if not remove_substreams else after_escape
    tuple_name = (
        "Tuple%28a%20UInt32%2C%20b%20UInt32%29"
        if escaped
        else "Tuple(a UInt32, b UInt32)"
    )
    expected_files = [
        f"w.{tuple_name}%2Ea.bin",
        f"w.{tuple_name}%2Eb.bin",
        "w.variant_discr.bin",
    ]
    assert sorted(files.splitlines()) == sorted(expected_files)
    if not remove_substreams:
        after = container_bash(
            f"cat {shlex.quote(renamed_path + 'columns_substreams.txt')}"
        )
        assert after == (
            "columns substreams version: 1\n"
            "1 columns:\n"
            "3 substreams for column `w`:\n"
            "\tw.variant_discr\n"
            f"\tw.{tuple_name}%2Ea\n"
            f"\tw.{tuple_name}%2Eb\n"
        )
    node.query(f"DROP TABLE {table} SYNC")
