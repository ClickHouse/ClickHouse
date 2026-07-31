#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
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
#
# no-fasttest: local-disk part-file surgery (see 04402/04404/04426/04427).
# no-object-storage/-shared/-replicated: relies on local on-disk file layout.
# no-random-merge-tree-settings: depends on standalone (non-packed) index files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Fabricate a part in the released-bug shape: skp_idx_txt.* on disk but absent from
# `checksums.txt`. Save the freshly written index files, DROP+re-ADD the index so the active
# part has no skp_idx entries in checksums, then re-inject the files on disk.
# Leaves the poisoned part's path in `CORRUPT_PART`, so callers never re-query it.
make_corrupted_part () {
    local tbl="$1"
    local mode="${2:-all}"
    CORRUPT_PART=
    # m mirrors 04426: a MATERIALIZED Map column, so `DROP COLUMN m` reaches
    # `MutateAllPartColumnsTask`. A scalar MATERIALIZED column is not enough -- dropping one
    # still takes the some-columns path (verified via `MutationAllPartColumns` in `system.part_log`).
    #
    # Granule-selective like t_ok below: the phrase sits only in the first 100 rows, i.e. in one
    # granule out of 20, so a `Granules: 1/20` assertion can tell a working positional index from
    # a silently declining one. A pure modulo fixture puts the phrase in every granule and makes
    # any pruning assertion vacuous. The modulo tokens stay for the `hasToken` counts.
    local paths data_path active
    paths=$(${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${tbl} SYNC;
    CREATE TABLE ${tbl}
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
             allow_experimental_text_index_phrase_search = 1;
    INSERT INTO ${tbl} (k, s, w) SELECT number, if(number < 100, 'needle alpha beta', concat('hello', number % 50, ' world', number % 50)), number FROM numbers(2000);
    SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = '${tbl}';
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active LIMIT 1")
    data_path=$(echo "${paths}" | sed -n 1p)
    active=$(echo "${paths}" | sed -n 2p)

    rm -rf "${data_path}/saved_${tbl}"
    mkdir -p "${data_path}/saved_${tbl}"
    cp "${active}"skp_idx_txt.* "${data_path}/saved_${tbl}/"

    CORRUPT_PART=$(${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE ${tbl} DROP INDEX txt SETTINGS mutations_sync = 2;
    ALTER TABLE ${tbl} ADD INDEX txt(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1;
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active LIMIT 1")

    if [ "${mode}" = side_streams_only ]; then
        # Base .idx pair deliberately omitted: a part poisoned this way is still corrupted, but
        # a presence check that probes only the base .idx/.idx2 reports it as index-free.
        cp "${data_path}/saved_${tbl}/"skp_idx_txt.dct.* "${data_path}/saved_${tbl}/"skp_idx_txt.pst.* \
           "${data_path}/saved_${tbl}/"skp_idx_txt.pos.* "${CORRUPT_PART}"
    else
        cp "${data_path}/saved_${tbl}/"skp_idx_txt.* "${CORRUPT_PART}"
    fi
}

# Both helpers take the part path, which every caller already holds from the statement that
# produced the part -- re-querying `system.parts` here would only add a client round trip.
orphan_on_disk () {
    local part="$1"
    if ls "${part}"skp_idx_txt.* >/dev/null 2>&1; then echo 1; else echo 0; fi
}

# Count the fabricated files one by one instead of globbing. A glob over
# skp_idx_txt.* stays green even if an entire substream silently stops being
# written -- in particular the positional .pos pair, which only exists while
# `support_phrase_search` is on -- and that would make the orphan-cleanup
# assertions vacuous for exactly the substreams this test is about. Expect 8:
# base, .dct, .pst and .pos, each with a data file and a mark file.
side_streams_on_disk () {
    local part="$1"
    local n=0 f
    for f in skp_idx_txt.idx skp_idx_txt.cmrk2 \
             skp_idx_txt.dct.idx skp_idx_txt.dct.cmrk2 \
             skp_idx_txt.pst.idx skp_idx_txt.pst.cmrk2 \
             skp_idx_txt.pos.idx skp_idx_txt.pos.cmrk2
    do
        if [ -e "${part}${f}" ]; then n=$((n + 1)); fi
    done
    echo "${n}"
}

# --- Path A: some-columns mutation (`ALTER UPDATE` of the non-indexed column w) ---
make_corrupted_part t_some
echo "A_corrupted_orphan_on_disk:"
orphan_on_disk "${CORRUPT_PART}"
echo "A_corrupted_side_streams_on_disk:"
side_streams_on_disk "${CORRUPT_PART}"
new_part=$(${CLICKHOUSE_CLIENT} -q "
ALTER TABLE t_some UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_some' AND active LIMIT 1")
echo "A_orphan_after_update:"
orphan_on_disk "${new_part}"
echo "A_check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_some SETTINGS check_query_single_value_result = 1;
    SELECT count() FROM t_some WHERE hasToken(s, 'hello10');
    DROP TABLE t_some SYNC"

# --- Path B: `DROP INDEX` on a corrupted part ---
make_corrupted_part t_drop
echo "B_corrupted_orphan_on_disk:"
orphan_on_disk "${CORRUPT_PART}"
echo "B_corrupted_side_streams_on_disk:"
side_streams_on_disk "${CORRUPT_PART}"
new_part=$(${CLICKHOUSE_CLIENT} -q "
ALTER TABLE t_drop DROP INDEX txt SETTINGS mutations_sync = 2;
SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_drop' AND active LIMIT 1")
echo "B_orphan_after_drop_index:"
orphan_on_disk "${new_part}"
echo "B_check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_drop SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_drop SYNC"

# --- Path C (no regression): a healthy text index survives a some-columns mutation ---
# Granule-selective on purpose: the phrase occurs only in the first 100 rows, i.e.
# in one granule out of 20, so the EXPLAIN assertion below can actually tell a
# working positional index from a silently declining one. A modulo fixture would
# put the phrase in every granule and make any pruning assertion vacuous.
ok_out=$(${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_ok SYNC;
CREATE TABLE t_ok (k UInt64, s String, w UInt64, INDEX txt(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         allow_experimental_text_index_phrase_search = 1;
INSERT INTO t_ok (k, s, w) SELECT number, if(number < 100, 'needle alpha beta', concat('hello', number % 50, ' world', number % 50)), number FROM numbers(2000);
ALTER TABLE t_ok UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_ok' AND active AND secondary_indices_marks_bytes > 0;
SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_ok' AND active LIMIT 1")
echo "C_healthy_index_survives:"
echo "${ok_out}" | sed -n 1p
# Every substream, including the positional pair, must survive the mutation
# individually -- an aggregate mark size stays positive even if .pos is lost.
echo "C_healthy_side_streams_on_disk:"
side_streams_on_disk "$(echo "${ok_out}" | sed -n 2p)"
echo "C_check_table:"
# `hasPhrase` reads the positional substream, so it fails if `.pos` was dropped, and the
# index must still PRUNE for it, which a count alone cannot show: a declining index
# would return the same 100 rows via a full scan.
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_ok SETTINGS check_query_single_value_result = 1;
    SELECT count() FROM t_ok WHERE hasToken(s, 'hello10');
    SELECT 'C_has_phrase:';
    SELECT count() FROM t_ok WHERE hasPhrase(s, 'needle alpha');
    SELECT 'C_has_phrase_prunes:';
    SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ok WHERE hasPhrase(s, 'needle alpha')) WHERE explain ILIKE '%Granules: 1/20%';
    DROP TABLE t_ok SYNC"

# --- Paths D and E: the same corruption with the base .idx pair ALSO missing ---
# Paths A-C keep the base skp_idx_txt.idx on disk, so a presence check that probes only the
# base extensions still sees the index and both repair paths run. Drop that pair and only the
# .dct/.pst/.pos side streams remain: the part is just as corrupted, but a base-only check
# reports it as index-free, so nothing collects the orphans and the full rewrite hardlinks
# them forward with no checksum entries. Expect 6 files (three substreams, data plus mark).
make_corrupted_part t_side side_streams_only
echo "D_corrupted_side_streams_on_disk:"
side_streams_on_disk "${CORRUPT_PART}"
new_part=$(${CLICKHOUSE_CLIENT} -q "
ALTER TABLE t_side UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_side' AND active LIMIT 1")
echo "D_orphan_after_update:"
orphan_on_disk "${new_part}"
echo "D_check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_side SETTINGS check_query_single_value_result = 1;
    SELECT count() FROM t_side WHERE hasToken(s, 'hello10');
    DROP TABLE t_side SYNC"

# Path E is the full-rewrite arm of the same shape: dropping the MATERIALIZED Map column m
# reaches `MutateAllPartColumnsTask`, which rebuilds the index from column data instead of
# leaving it absent (04426 asserts the same repair for a single-stream minmax index). So here
# the index files are expected to be BACK and checksummed, and to prune again -- unlike paths
# A/B/D, where the orphans are removed and the index stays absent until `MATERIALIZE INDEX`.
make_corrupted_part t_side_full side_streams_only
echo "E_corrupted_side_streams_on_disk:"
side_streams_on_disk "${CORRUPT_PART}"
new_part=$(${CLICKHOUSE_CLIENT} -q "
ALTER TABLE t_side_full DROP COLUMN m SETTINGS mutations_sync = 2;
SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_side_full' AND active LIMIT 1")
# All 8 files, not just the 6 injected ones: a rebuild writes the base pair too.
echo "E_side_streams_after_full_rewrite:"
side_streams_on_disk "${new_part}"
echo "E_check_table:"
# The rebuilt index must actually prune, which the file count alone cannot show.
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_side_full SETTINGS check_query_single_value_result = 1;
    SELECT count() FROM t_side_full WHERE hasToken(s, 'hello10');
    SELECT 'E_rebuilt_index_prunes:';
    SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_side_full WHERE hasPhrase(s, 'needle alpha')) WHERE explain ILIKE '%Granules: 1/20%';
    DROP TABLE t_side_full SYNC"

# --- Path F: a sibling index registers a file the corrupted text index also addresses ---
# With `escape_index_filenames` = 0 an index NAME may equal a text substream name, so a minmax
# index called `a.pst` and the `.pst` substream of a text index `a` both address
# `skp_idx_a.pst.cmrk2`. Only the minmax writes it, and it is in `checksums.txt`. Resolving
# orphan candidates against storage alone claims that file for the corrupted text index, so it
# is skipped while its checksum entry survives -> `CHECK TABLE` fails with NO_FILE_IN_DATA_PART.
# The registered owner here is dropped by the very mutation under test, so it cannot be found
# in the (already post-drop) metadata; only checksum membership identifies the file.
# The sibling name is the only variable: `b` is the control, `a.pst` collides.
run_sibling_owns_file_case () {
    local label="$1" sib="$2"
    local tbl="t_own_${label}"

    local paths dp act
    paths=$(${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${tbl} SYNC;
    CREATE TABLE ${tbl}
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
             allow_experimental_text_index_phrase_search = 1;
    INSERT INTO ${tbl} (k, s, w) SELECT number, concat('hello', number % 50, ' world', number % 50), number FROM numbers(500);
    SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = '${tbl}';
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active LIMIT 1")
    dp=$(echo "${paths}" | sed -n 1p)
    act=$(echo "${paths}" | sed -n 2p)
    rm -rf "${dp}/saved_${tbl}"
    mkdir -p "${dp}/saved_${tbl}"
    cp "${act}"skp_idx_a.* "${dp}/saved_${tbl}/"

    # Corrupt the text index while NO sibling exists, so nothing inherits a checksum entry for
    # its files, then add the healthy sibling. Order matters: adding the sibling first would let
    # it register a name the text index also addresses before the entries are stripped.
    # The sibling must be materialized and registered, else its file is not in `checksums.txt`,
    # the collision never arises, and the final `CHECK TABLE` passes for the wrong reason.
    local out cor f bn
    out=$(${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE ${tbl} DROP INDEX a SETTINGS mutations_sync = 2;
    ALTER TABLE ${tbl} ADD INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1;
    ALTER TABLE ${tbl} ADD INDEX \`${sib}\` w TYPE minmax GRANULARITY 1;
    ALTER TABLE ${tbl} MATERIALIZE INDEX \`${sib}\` SETTINGS mutations_sync = 2;
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active LIMIT 1;
    SELECT count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = '${tbl}' AND name = '${sib}' AND marks_bytes > 0;
    SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE w = 42)
    WHERE explain ILIKE '%Granules: 1/5%'")
    cor=$(echo "${out}" | sed -n 1p)

    # Measured BEFORE reinjection, so it discriminates: only the colliding name makes the sibling
    # write the contested `skp_idx_a.pst.cmrk2` (0 for the control, 1 for the collision). After
    # reinjection the text fixture supplies that name in either case, so the same check there
    # would read 1 for both arms and prove nothing.
    echo "${label}_sibling_owns_contested_name:"
    if [ -e "${cor}skp_idx_a.pst.cmrk2" ]; then echo 1; else echo 0; fi
    echo "${label}_sibling_registered_and_prunes:"
    echo "${out}" | sed -n '2,3p'

    # Re-inject the text index's files, never overwriting one the healthy sibling wrote.
    for f in "${dp}/saved_${tbl}/"skp_idx_a.*; do
        bn=$(basename "${f}")
        if [ -e "${cor}${bn}" ]; then continue; fi
        cp "${f}" "${cor}"
    done

    local new_part
    new_part=$(${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE ${tbl} DROP INDEX \`${sib}\` SETTINGS mutations_sync = 2;
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active LIMIT 1")

    # The corrupted text index's own orphans must still be cleaned up, so a green `CHECK TABLE`
    # cannot come from the repair silently doing nothing. Its base pair is never contested.
    echo "${label}_text_orphans_removed:"
    if [ -e "${new_part}skp_idx_a.idx" ] || [ -e "${new_part}skp_idx_a.dct.idx" ]; then echo 1; else echo 0; fi
    echo "${label}_check_table:"
    ${CLICKHOUSE_CLIENT} -q "CHECK TABLE ${tbl} SETTINGS check_query_single_value_result = 1;
        DROP TABLE ${tbl} SYNC"
}

run_sibling_owns_file_case F_control b
run_sibling_owns_file_case F_collide a.pst
