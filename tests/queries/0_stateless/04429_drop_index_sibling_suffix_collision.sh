#!/usr/bin/env bash
# Tags: no-fasttest, no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database
#
# `no-random-merge-tree-settings`: the test pins `escape_index_filenames` and
# `packed_skip_index_max_bytes`, which are exactly the settings the collision
# depends on.
#
# `no-fasttest`: the test reads on-disk part layout directly - individual `skp_idx_*` filenames and
# the size of `skp_idx.packed` - which is only meaningful on a local disk. It performs no part-file
# surgery. (The earlier justification here, that the text index type is unregistered in the Fast
# test build, was wrong: `registerCreator` for `text` in `MergeTreeIndices` is unconditional.)
#
# `DROP INDEX` must not delete files owned by a surviving index.
#
# The `DROP INDEX` bookkeeping has to enumerate substream suffixes speculatively
# (the dropped index's type is already gone from metadata by then, so its real
# substream list is unavailable). With `escape_index_filenames` = 0 the stream name
# is the index name verbatim, so "drop index a" + speculative suffix ".pos"
# addresses skp_idx_a.pos.*, which is the on-disk name of an unrelated surviving
# index literally named `a.pos`. Both the standalone and the packed-archive path
# must skip any candidate a surviving index owns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# $1: label, $2: `escape_index_filenames`, $3: `packed_skip_index_max_bytes`
run_case() {
    local label="$1" escape="$2" packed="$3"
    local tbl="t_coll_${label}"

    # v = k and w = k are monotone, so each minmax index prunes a point query to
    # a single granule. `index_granularity` = 100 over 500 rows gives 5 granules.
    ${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${tbl} SYNC;
    CREATE TABLE ${tbl}
    (
        k UInt64,
        v UInt64,
        w UInt64,
        INDEX a v TYPE minmax GRANULARITY 1,
        INDEX \`a.pos\` w TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             index_granularity = 100, replace_long_file_name_to_hash = 0,
             escape_index_filenames = ${escape},
             packed_skip_index_max_bytes = ${packed};
    INSERT INTO ${tbl} (k, v, w) SELECT number, number, number FROM numbers(500);
    OPTIMIZE TABLE ${tbl} FINAL"

    ${CLICKHOUSE_CLIENT} -q "
    SELECT '${label}_before_both_indices_prune:';
    SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE v = 42) WHERE explain ILIKE '%Granules: 1/5%';
    SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE w = 42) WHERE explain ILIKE '%Granules: 1/5%'"

    # `mutations_sync` = 2 makes the drop complete before the next statement in the
    # batch runs, so the assertions below still observe the post-drop state. The
    # dropped index is gone, and the sibling survived with its files intact enough
    # to still prune.
    ${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE ${tbl} DROP INDEX a SETTINGS mutations_sync = 2;
    SELECT '${label}_dropped_index_gone:';
    SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}' AND name = 'a';
    SELECT '${label}_sibling_survives:';
    SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}' AND name = 'a.pos';
    SELECT '${label}_sibling_still_prunes:';
    SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE w = 42) WHERE explain ILIKE '%Granules: 1/5%';
    SELECT '${label}_check_table:';
    CHECK TABLE ${tbl};
    DROP TABLE ${tbl} SYNC" | cut -f2
}

# The collision also runs the other way: dropping an index literally named
# `a.pos` addresses skp_idx_a.pos.*, which is the positional substream of a text
# index named `a`. Guarding only the dropped-name side would leave that open, so
# the surviving text index would lose its positional files.
run_inverse_case() {
    local label="$1" escape="$2"
    local tbl="t_inv_${label}"

    ${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${tbl} SYNC;
    CREATE TABLE ${tbl}
    (
        k UInt64,
        s String,
        w UInt64,
        INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
        INDEX \`a.pos\` w TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             index_granularity = 100, replace_long_file_name_to_hash = 0,
             packed_skip_index_max_bytes = 0,
             escape_index_filenames = ${escape},
             allow_experimental_text_index_phrase_search = 1;
    INSERT INTO ${tbl} (k, s, w) SELECT number, concat('hello', number % 50, ' world', number % 50), number FROM numbers(500);
    OPTIMIZE TABLE ${tbl} FINAL"

    local part
    part=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active ORDER BY name LIMIT 1")

    # The text index's positional substream files, by their real on-disk names. The
    # assertion is file survival rather than a `hasPhrase` result on purpose: with
    # `escape_index_filenames` = 0 the minmax index `a.pos` and the text index's own
    # `.pos` substream want the same mark filename, so this table is already
    # unreadable for phrase search before any `DROP INDEX` runs (that write-time
    # collision is a separate, pre-existing issue). What must hold here is that
    # `DROP INDEX` does not delete files the surviving text index owns.
    # The text index's stream base is `skp_idx_a` under either escaping mode (there is
    # no dot in its own name to escape), so its positional substream files are:
    local pos_data="skp_idx_a.pos.idx"
    local pos_mark="skp_idx_a.pos.cmrk2"

    echo "${label}_text_pos_files_before:"
    if [ -e "${part}${pos_data}" ] && [ -e "${part}${pos_mark}" ]; then echo 1; else echo 0; fi

    # `mutations_sync` = 2 completes the drop before the following statements run, so
    # the new part path and the index counts below are all post-drop.
    local new_part
    new_part=$(${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE ${tbl} DROP INDEX \`a.pos\` SETTINGS mutations_sync = 2;
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active ORDER BY name LIMIT 1")

    ${CLICKHOUSE_CLIENT} -q "
    SELECT '${label}_dropped_index_gone:';
    SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}' AND name = 'a.pos';
    SELECT '${label}_text_index_survives:';
    SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}' AND name = 'a'"
    echo "${label}_text_pos_files_after:"
    if [ -e "${new_part}${pos_data}" ] && [ -e "${new_part}${pos_mark}" ]; then echo 1; else echo 0; fi
    # The survivor must not over-claim by EXTENSION either: the text index stores its
    # `.pos` substream as `.idx`, so the dropped minmax index's own `.idx2` file has to
    # be removed rather than protected.
    echo "${label}_dropped_minmax_file_not_leaked:"
    if [ -e "${new_part}skp_idx_a.pos.idx2" ] || [ -e "${new_part}skp_idx_a%2Epos.idx2" ]; then echo 1; else echo 0; fi
    echo "${label}_check_table:"
    ${CLICKHOUSE_CLIENT} -q "CHECK TABLE ${tbl}; DROP TABLE ${tbl} SYNC" | cut -f2
}

# Dropping the index whose NAME carries the suffix, where the sibling is an
# ordinary minmax index that owns no `.pos` substream at all. The survivor must
# not over-claim skp_idx_a.pos.*, or the dropped index's own files leak into the
# new part (and a later re-ADD without `MATERIALIZE INDEX` could then read stale
# index data).
run_suffix_named_drop_case() {
    local label="$1" escape="$2" packed="$3"
    local tbl="t_sfx_${label}"
    # Only the packed branch builds a reference table, so this stays empty otherwise.
    local drop_ref=""

    ${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${tbl} SYNC;
    CREATE TABLE ${tbl}
    (
        k UInt64,
        v UInt64,
        w UInt64,
        INDEX a v TYPE minmax GRANULARITY 1,
        INDEX \`a.pos\` w TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             index_granularity = 100, replace_long_file_name_to_hash = 0,
             escape_index_filenames = ${escape},
             packed_skip_index_max_bytes = ${packed},
             columns_and_secondary_indices_sizes_lazy_calculation = 0;
    INSERT INTO ${tbl} (k, v, w) SELECT number, number, number FROM numbers(500);
    OPTIMIZE TABLE ${tbl} FINAL;
    ALTER TABLE ${tbl} DROP INDEX \`a.pos\` SETTINGS mutations_sync = 2"

    # One call returns the post-drop part path on line 1 and the dropped-index count on
    # line 2; the count is echoed under its label to keep the output order unchanged.
    local facts part
    facts=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active ORDER BY name LIMIT 1;
    SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}' AND name = 'a.pos'")
    part=$(echo "${facts}" | sed -n 1p)

    echo "${label}_dropped_index_gone:"
    echo "${facts}" | sed -n 2p
    # With packing enabled the dropped index's data is a virtual member of
    # `skp_idx.packed`, so a filesystem check cannot see it. Compare against a table
    # that only ever had the surviving index: an identical archive size and an
    # identical accounted index size mean the member really went away, whereas a
    # retained member would make both larger.
    echo "${label}_dropped_files_not_leaked:"
    if [ "${packed}" = "0" ]; then
        if [ -e "${part}skp_idx_a.pos.idx2" ] || [ -e "${part}skp_idx_a%2Epos.idx2" ]; then echo 1; else echo 0; fi
    else
        ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS ${tbl}_ref SYNC;
        CREATE TABLE ${tbl}_ref (k UInt64, v UInt64, w UInt64, INDEX a v TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 index_granularity = 100, replace_long_file_name_to_hash = 0,
                 escape_index_filenames = ${escape},
                 packed_skip_index_max_bytes = ${packed},
                 columns_and_secondary_indices_sizes_lazy_calculation = 0;
        INSERT INTO ${tbl}_ref (k, v, w) SELECT number, number, number FROM numbers(500);
        OPTIMIZE TABLE ${tbl}_ref FINAL"
        # One call returns the reference part path and both accounted index sizes, in
        # that order, so the comparison below still reads the same three values.
        local ref_facts ref_part ref_size part_size ref_archive part_archive
        ref_facts=$(${CLICKHOUSE_CLIENT} -q "
        SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}_ref' AND active ORDER BY name LIMIT 1;
        SELECT secondary_indices_compressed_bytes FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active;
        SELECT secondary_indices_compressed_bytes FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}_ref' AND active")
        ref_part=$(echo "${ref_facts}" | sed -n 1p)
        part_size=$(echo "${ref_facts}" | sed -n 2p)
        ref_size=$(echo "${ref_facts}" | sed -n 3p)
        ref_archive=$(stat -c%s "${ref_part}skp_idx.packed")
        part_archive=$(stat -c%s "${part}skp_idx.packed")
        if [ "${ref_archive}" = "${part_archive}" ] && [ "${part_size}" = "${ref_size}" ]
        then echo 0; else echo 1; fi
        # Dropped in the closing batch below rather than in its own call.
        drop_ref="DROP TABLE ${tbl}_ref SYNC;"
    fi
    ${CLICKHOUSE_CLIENT} -q "
    SELECT '${label}_survivor_still_prunes:';
    SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE v = 42) WHERE explain ILIKE '%Granules: 1/5%';
    SELECT '${label}_check_table:';
    CHECK TABLE ${tbl};
    ${drop_ref}
    DROP TABLE ${tbl} SYNC" | cut -f2
}

# A surviving index that is NOT packed must not protect a member of the archive on behalf of the
# packed index being dropped. A text index is never packed (`MergeTreeDataPartWriterOnDisk` excludes
# it), so a standalone text index `a` declaring a `.pst` substream must not shield the packed minmax
# index `a.pst`, whose member really does live in `skp_idx.packed`.
#
# `.pst` rather than `.pos` on purpose: the text index writes its `.pst` substream to a standalone
# file while the minmax member is virtual, so the two never contend for one filename and this case is
# not masked by the separate write-time collision noted in `run_inverse_case`. The third index
# `keeper` is packed and survives, which keeps the archive present after the drop.
run_packed_survivor_not_packed_case() {
    local label="$1"
    local tbl="t_pkw_${label}"
    local ref="${tbl}_ref"

    local create_settings="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             index_granularity = 100, replace_long_file_name_to_hash = 0,
             escape_index_filenames = 0, packed_skip_index_max_bytes = 1048576,
             columns_and_secondary_indices_sizes_lazy_calculation = 0"

    # The reference carries the same two SURVIVING indices and never had the dropped one, so after a
    # correct drop both archives must hold exactly the same members.
    ${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${tbl} SYNC;
    DROP TABLE IF EXISTS ${ref} SYNC;
    CREATE TABLE ${tbl}
    (
        k UInt64,
        s String,
        w UInt64,
        v UInt64,
        INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
        INDEX \`a.pst\` w TYPE minmax GRANULARITY 1,
        INDEX keeper v TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k SETTINGS ${create_settings};
    CREATE TABLE ${ref}
    (
        k UInt64,
        s String,
        w UInt64,
        v UInt64,
        INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
        INDEX keeper v TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k SETTINGS ${create_settings};
    INSERT INTO ${tbl} (k, s, w, v) SELECT number, concat('hello', number % 50, ' world', number % 50), number, number FROM numbers(500);
    OPTIMIZE TABLE ${tbl} FINAL;
    INSERT INTO ${ref} (k, s, w, v) SELECT number, concat('hello', number % 50, ' world', number % 50), number, number FROM numbers(500);
    OPTIMIZE TABLE ${ref} FINAL;
    ALTER TABLE ${tbl} DROP INDEX \`a.pst\` SETTINGS mutations_sync = 2"

    # One call returns, in order: both part paths, then the two index counts. The counts
    # are echoed under their labels below so the output order is unchanged.
    local facts part ref_part
    facts=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${tbl}' AND active ORDER BY name LIMIT 1;
    SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${ref}' AND active ORDER BY name LIMIT 1;
    SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}' AND name = 'a.pst';
    SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '${tbl}'")
    part=$(echo "${facts}" | sed -n 1p)
    ref_part=$(echo "${facts}" | sed -n 2p)

    echo "${label}_dropped_index_gone:"
    echo "${facts}" | sed -n 3p
    echo "${label}_survivors_present:"
    echo "${facts}" | sed -n 4p
    # A filesystem check cannot see a virtual archive member, so compare the archive against the
    # reference: a retained member of the dropped index makes this archive strictly larger.
    echo "${label}_dropped_member_not_retained:"
    if [ "$(stat -c%s "${part}skp_idx.packed")" = "$(stat -c%s "${ref_part}skp_idx.packed")" ]; then echo 0; else echo 1; fi
    ${CLICKHOUSE_CLIENT} -q "
    SELECT '${label}_survivor_still_prunes:';
    SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ${tbl} WHERE v = 42) WHERE explain ILIKE '%Granules: 1/5%';
    DROP TABLE ${tbl} SYNC;
    DROP TABLE ${ref} SYNC"
}

# Unescaped names are the shape that collides: the stream name is the index name
# verbatim, so `a` + ".pos" == the stream name of index `a.pos`.
run_case "standalone_unescaped" 0 0
# Escaping makes the sibling's file skp_idx_a%2Epos.*, so no collision is
# possible; kept as a control that the guard does not break the ordinary path.
run_case "standalone_escaped" 1 0
# Same collision, but the files live inside `skp_idx.packed` and are removed by the
# archive filter rather than by a rename-to-empty.
run_case "packed_unescaped" 0 1048576
# Inverse direction: drop the index whose name IS the sibling substream suffix.
run_inverse_case "inverse_unescaped" 0
run_inverse_case "inverse_escaped" 1
# Same suffix collision, but the dropped index is the one whose NAME ends in the
# suffix and the survivor owns no such substream.
run_suffix_named_drop_case "suffixdrop_unescaped" 0 0
run_suffix_named_drop_case "suffixdrop_packed" 0 1048576
# A never-packed survivor must not shield the packed dropped index's archive member.
run_packed_survivor_not_packed_case "packedsurvivor"
