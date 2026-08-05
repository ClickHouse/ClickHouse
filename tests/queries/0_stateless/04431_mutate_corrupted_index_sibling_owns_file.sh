#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
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
# `no-fasttest`: local-disk part-file surgery.
# no-object-storage/-shared/-replicated: relies on the local on-disk file layout.
# no-random-merge-tree-settings: pins `escape_index_filenames` and `packed_skip_index_max_bytes`,
# which are exactly the settings the collision depends on.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Fabricate a part where ONLY the minmax index `a.pos` is corrupted: its file is on disk but has no
# per-file entries in `checksums.txt`. The text index `a` stays fully healthy and checksummed, so its
# `skp_idx_a.pos.idx` is the sibling file the corrupted index's legacy probe reaches.
#
# `packed_skip_index_max_bytes` = 0 is mandatory: the classification short-circuits to "resolvable"
# for any index living in `skp_idx.packed`, which would make every assertion below vacuous.
# `support_phrase_search` = 1 plus `allow_experimental_text_index_phrase_search` = 1 are mandatory
# too - without them the text index declares no `.pos` substream and there is no collision at all.
make_corrupted_part () {
    local tbl="$1"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${tbl} SYNC"
    ${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${tbl}
    (
        k UInt64,
        s String,
        w UInt64,
        u UInt64,
        m Map(String, UInt64) MATERIALIZED map('a', k),
        INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
        INDEX \`a.pos\` w TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             index_granularity = 100, replace_long_file_name_to_hash = 0,
             escape_index_filenames = 0, packed_skip_index_max_bytes = 0,
             columns_and_secondary_indices_sizes_lazy_calculation = 0,
             allow_experimental_text_index_phrase_search = 1"

    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${tbl} (k, s, w, u) SELECT number, concat('hello', number % 50, ' world', number % 50), number, number FROM numbers(500)"
    ${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE ${tbl} FINAL"

    local data_path active
    data_path=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = '${tbl}'")
    active=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active ORDER BY name LIMIT 1")

    rm -rf "${data_path}/saved_${tbl}"
    mkdir -p "${data_path}/saved_${tbl}"
    # ONLY the minmax index's own `.idx2` payload. Its mark file `skp_idx_a.pos.cmrk2` is the same
    # filename the text index's positional substream writes, so copying it back would overwrite the
    # healthy sibling's mark and corrupt the very index this test asserts stays intact.
    cp "${active}"skp_idx_a.pos.idx2 "${data_path}/saved_${tbl}/"

    # DROP + re-ADD makes the active part carry no checksums entries for `a.pos`, then the saved
    # files are re-injected on disk. Re-ADD without `MATERIALIZE INDEX` leaves it unmaterialized,
    # which is the released-bug shape.
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${tbl} DROP INDEX \`a.pos\` SETTINGS mutations_sync = 2"
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE ${tbl} ADD INDEX \`a.pos\` w TYPE minmax GRANULARITY 1"

    local corrupt
    corrupt=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active ORDER BY name LIMIT 1")
    cp "${data_path}/saved_${tbl}/skp_idx_a.pos.idx2" "${corrupt}"
}

orphan_on_disk () {
    local tbl="$1"
    local part
    part=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active ORDER BY name LIMIT 1")
    if [ -e "${part}skp_idx_a.pos.idx2" ]; then echo 1; else echo 0; fi
}

# Enumerate the text index's substreams one by one rather than globbing skp_idx_a.*: a glob stays
# green even if an entire substream stops being written.
#
# The positional pair is deliberately EXCLUDED. With `escape_index_filenames` = 0 the minmax index
# `a.pos` and the text index's own `.pos` substream want the same filenames, so this table already
# loses `skp_idx_a.pos.idx` on any rewrite, before this test's corruption is introduced. That
# write-time collision is a separate pre-existing issue (see 04429), and asserting its survival here
# would encode existing breakage as expected behaviour. Expect 6: base, `.dct` and `.pst`, each with
# a data file and a mark file.
text_streams_on_disk () {
    local tbl="$1"
    local part n=0 f
    part=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active ORDER BY name LIMIT 1")
    for f in skp_idx_a.idx skp_idx_a.cmrk2 \
             skp_idx_a.dct.idx skp_idx_a.dct.cmrk2 \
             skp_idx_a.pst.idx skp_idx_a.pst.cmrk2
    do
        if [ -e "${part}${f}" ]; then n=$((n + 1)); fi
    done
    echo "${n}"
}

# --- Path A: full-part rewrite (`DROP COLUMN` of the MATERIALIZED `Map` column m) ---
# `m` is a MATERIALIZED `Map`, i.e. a column with dynamic subcolumns, so dropping it forces
# `MutateAllPartColumnsTask` (same device as 04426). Verified by `MutationAllPartColumns`: an
# ordinary `UInt64` column would be handled as a file rename and take the some-columns path
# instead, leaving this site unexercised.
#
# The dropped column is deliberately NOT the indexed one. An `ALTER UPDATE w` would put the index
# in `materialized_indices`, which forces a recalculate on its own and would make the assertion
# vacuous - it would stay green even with the classification broken.
make_corrupted_part t_full
echo "A_corrupted_orphan_on_disk:"
orphan_on_disk t_full
echo "A_sibling_text_streams_before:"
text_streams_on_disk t_full
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_full DROP COLUMN m SETTINGS mutations_sync = 2"
# The index must be REBUILT, so its file is present again - but this time as a checksummed member of
# the new part rather than as the hardlinked-forward orphan. `CHECK TABLE` is what separates the two:
# with the classification counting the sibling's file as evidence of health the index is not
# rebuilt, the unchecksummed orphan is carried into the new part, and this returns 0.
echo "A_index_file_present:"
orphan_on_disk t_full
echo "A_check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_full SETTINGS check_query_single_value_result = 1"
# A rebuilt minmax index prunes; a missing or stale one cannot. `w` = `k` is monotone and
# `index_granularity` = 100 over 500 rows gives 5 granules.
echo "A_rebuilt_index_prunes:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_full WHERE w = 42) WHERE explain ILIKE '%Granules: 1/5%'"
echo "A_sibling_text_streams_after:"
text_streams_on_disk t_full
echo "A_both_indices_present:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_full'"
echo "A_rows:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_full"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_full SYNC"

# --- Path B: some-columns mutation (`ALTER UPDATE` of the non-indexed column u) ---
# Only `u` is rewritten, so the orphan scan decides: the corrupted index's orphan file must be
# RECORDED and left behind instead of being hardlinked into the new part.
make_corrupted_part t_some
echo "B_corrupted_orphan_on_disk:"
orphan_on_disk t_some
echo "B_sibling_text_streams_before:"
text_streams_on_disk t_some
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_some UPDATE u = u + 1 WHERE 1 SETTINGS mutations_sync = 2"
echo "B_orphan_after_update:"
orphan_on_disk t_some
echo "B_check_table:"
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_some SETTINGS check_query_single_value_result = 1"
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
echo "B_text_index_registered:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_some' AND name = 'a' AND marks_bytes > 0 AND data_compressed_bytes > 0"
echo "B_sibling_text_streams_after:"
text_streams_on_disk t_some
echo "B_rows:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_some"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_some SYNC"

