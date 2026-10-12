#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: EXPLAIN output and active part lists differ across replicas.
#
# Mutation matrix for packed skip indices. Every scenario does the same thing:
#   1. create a table with a packed skip index and populate it,
#   2. run one mutation,
#   3. read the resulting state back and print a short, stable summary.
#
# State is checked via system tables only (no on-disk file listing), so the assertions also
# hold for object-storage-backed deployments:
#   - system.parts.secondary_indices_compressed_bytes: >0 while a packed index is present,
#     0 once the last one is dropped ("materialized" / "cleared" below).
#   - a post-mutation count() over an index-prunable predicate, compared to an expected value.
#   - EXPLAIN indexes=1: whether the surviving skip index still prunes granules.
#   - CHECK TABLE: checksums stay consistent after the mutation.
# Absolute file/granule counts are never asserted - random merge-tree settings perturb them.
#
# Coverage matrix: Compact x Wide; sole-packed vs mixed (packed + per-file); ALTER UPDATE on
# indexed and non-indexed columns; lightweight DELETE; DROP INDEX of a packed index, of a
# per-file index, and of all packed indices; ALTER ADD COLUMN (metadata-only).
#
# Each scenario uses a single client invocation, because on sanitizer builds client process
# startup dominates the runtime. To keep that readable, the batched query labels every result:
# scalar metrics are emitted as "tag<TAB>value" rows, and the CHECK TABLE / EXPLAIN outputs
# follow "@check" / "@explain" marker rows. Bash then picks each value out by its label instead
# of by position.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT"
SYNC_ALTER="--mutations_sync=2 --alter_sync=2"

# Classify how index $1 appears in an "EXPLAIN indexes=1" plan read from stdin:
#   filtered  - the index is in the plan and pruned some granules (kept < total)
#   no_filter - the index is in the plan but matched every granule (kept == total)
#   missing   - the index is not in the plan (dropped / not usable)
explain_index_status() {
    awk -v want="$1" '
        /^[[:space:]]*Name:/ {
            name = $0; sub(/.*Name: /, "", name); sub(/[[:space:]]+$/, "", name)
            looking_at_index = (name == want)
        }
        looking_at_index && /Granules:/ {
            split($NF, g, "/")              # $NF is "kept/total"
            print (g[1] != g[2]) ? "filtered" : "no_filter"
            seen = 1; exit
        }
        END { if (!seen) print "missing" }'
}

# Pull a labelled scalar ("tag<TAB>value") out of a batched result.
tagged_value() { awk -F'\t' -v tag="$2" '$1 == tag { print $2; exit }' <<< "$1"; }
# Pull the single line that follows a "@marker" row out of a batched result.
line_after_marker() { awk -v m="$2" '$0 == m { getline; print; exit }' <<< "$1"; }
# Pull every line that follows a "@marker" row (used for the multi-line EXPLAIN plan).
block_after_marker() { awk -v m="$2" 'found; $0 == m { found = 1 }' <<< "$1"; }

# $1 label, $2 table, $3 mutation, $4 where-predicate, $5 expected count, $6 index name,
# $7 setup SQL (DROP/CREATE/INSERT). See the per-scenario comments below for what each verifies.
run_scenario() {
    local label="$1" table="$2" mutation="$3" where="$4" expected="$5" idx="$6" setup="$7" packed_ref_files="$8"

    local out
    out=$($CLIENT --multiquery $SYNC_ALTER -q "
        $setup
        SELECT 'pre_files', files FROM system.parts
            WHERE database = currentDatabase() AND table = '$table' AND active ORDER BY name LIMIT 1;
        $mutation;

        SELECT 'sec_bytes', secondary_indices_compressed_bytes FROM system.parts
            WHERE database = currentDatabase() AND table = '$table' AND active ORDER BY name LIMIT 1;
        SELECT 'count', count() FROM $table WHERE $where;
        SELECT '@check';
        CHECK TABLE $table SETTINGS check_query_single_value_result = 1;
        SELECT '@explain';
        EXPLAIN indexes = 1 SELECT count() FROM $table WHERE $where;
    ")

    local sec_bytes count check granules
    sec_bytes=$(tagged_value "$out" sec_bytes)
    count=$(tagged_value "$out" count)
    check=$(line_after_marker "$out" '@check')
    granules=$(block_after_marker "$out" '@explain' | explain_index_status "$idx")

    echo "[$label]"
    # When a packed-layout baseline ($8) is given, assert the table really is in the mixed layout
    # before the mutation: the part has more files than the all-packed twin, i.e. the larger
    # set index spilled to standalone per-file substreams. This guards the per-file update/drop
    # paths, which packed-only data would never exercise.
    if [[ -n "$packed_ref_files" ]]; then
        local pre_files
        pre_files=$(tagged_value "$out" pre_files)
        echo "  set_index_layout: $( ((pre_files > packed_ref_files)) && echo per_file || echo packed )"
    fi
    echo "  secondary_indices: $( ((sec_bytes > 0)) && echo materialized || echo cleared )"
    echo "  count=$count expected=$expected granules($idx)=$granules check=$check"
}

# Shared mixed-layout schema/data for scenarios D and G: a small packed minmax (m_v) plus a large
# set index (s_s) over high-cardinality strings. With a low packed_skip_index_max_bytes the set
# spills to a standalone per-file substream while the minmax stays in skp_idx.packed. The
# all-packed twin below (high threshold) gives the baseline file count the scenarios compare
# against to prove the spill happened.
MIXED_COLUMNS="id UInt64, v UInt64, s String, INDEX m_v v TYPE minmax GRANULARITY 1, INDEX s_s s TYPE set(0) GRANULARITY 1"
MIXED_INSERT="SELECT number, number * 7, toString(number) FROM numbers(20000)"
PACKED_MIXED_FILES=$($CLIENT --multiquery -q "
    DROP TABLE IF EXISTS ref_packed_mixed;
    CREATE TABLE ref_packed_mixed ($MIXED_COLUMNS) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 4194304, index_granularity = 1024;
    INSERT INTO ref_packed_mixed $MIXED_INSERT;
    SELECT files FROM system.parts
        WHERE database = currentDatabase() AND table = 'ref_packed_mixed' AND active ORDER BY name LIMIT 1;
    DROP TABLE ref_packed_mixed;
")

# Scenario A: Wide, sole packed minmax, ALTER UPDATE on the indexed column.
# Archive rebuilt by the writer; the minmax is recomputed so it stays materialized and usable.
run_scenario "A_wide_update_indexed" a_wide_indexed \
    "ALTER TABLE a_wide_indexed UPDATE v = v + 1 WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v \
    "DROP TABLE IF EXISTS a_wide_indexed;
     CREATE TABLE a_wide_indexed (
         id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
     ) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', index_granularity = 1024;
     INSERT INTO a_wide_indexed SELECT number, number * 7 FROM numbers(2000);"

# Scenario B: Wide, sole packed minmax, ALTER UPDATE on a NON-indexed column.
# Archive hardlinked unchanged; m_v still prunes.
run_scenario "B_wide_update_unindexed" b_wide_unindexed \
    "ALTER TABLE b_wide_unindexed UPDATE s = concat(s, '_x') WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v \
    "DROP TABLE IF EXISTS b_wide_unindexed;
     CREATE TABLE b_wide_unindexed (
         id UInt64, v UInt64, s String, INDEX m_v v TYPE minmax GRANULARITY 1
     ) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', index_granularity = 1024;
     INSERT INTO b_wide_unindexed SELECT number, number * 7, toString(number % 50) FROM numbers(2000);"

# Scenario C: Wide, packed minmax, lightweight DELETE. Index data untouched; m_v still prunes.
run_scenario "C_wide_lightweight_delete" c_wide_lwd \
    "DELETE FROM c_wide_lwd WHERE id < 100" \
    "v BETWEEN 70 AND 700" 1 m_v \
    "DROP TABLE IF EXISTS c_wide_lwd;
     CREATE TABLE c_wide_lwd (
         id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
     ) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', index_granularity = 1024;
     INSERT INTO c_wide_lwd SELECT number, number * 7 FROM numbers(2000);"

# Scenario D: Wide, mixed (packed minmax + per-file set), ALTER UPDATE on the set's column.
# Per-file set rebuilt, archive untouched; m_v still prunes. The set_index_layout line asserts
# s_s really is per-file before the update (otherwise this would exercise the packed path).
run_scenario "D_wide_mixed_update_set" d_wide_mixed_update_set \
    "ALTER TABLE d_wide_mixed_update_set UPDATE s = concat(s, '_x') WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v \
    "DROP TABLE IF EXISTS d_wide_mixed_update_set;
     CREATE TABLE d_wide_mixed_update_set ($MIXED_COLUMNS) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 4096, index_granularity = 1024;
     INSERT INTO d_wide_mixed_update_set $MIXED_INSERT;" \
    "$PACKED_MIXED_FILES"

# Scenario E: Wide, mixed, ALTER UPDATE on the minmax's column. Archive recomputed, the per-file
# set is left in place; m_v still prunes.
run_scenario "E_wide_mixed_update_minmax" e_wide_mixed_update_minmax \
    "ALTER TABLE e_wide_mixed_update_minmax UPDATE v = v + 1 WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v \
    "DROP TABLE IF EXISTS e_wide_mixed_update_minmax;
     CREATE TABLE e_wide_mixed_update_minmax ($MIXED_COLUMNS) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 4096, index_granularity = 1024;
     INSERT INTO e_wide_mixed_update_minmax $MIXED_INSERT;" \
    "$PACKED_MIXED_FILES"

# Scenario F: Wide, two packed minmax indices, DROP one. Archive rebuilt smaller in place;
# the surviving m_w still prunes.
run_scenario "F_wide_drop_one_of_two_packed" f_wide_drop_packed \
    "ALTER TABLE f_wide_drop_packed DROP INDEX m_v" \
    "w BETWEEN 110 AND 1100" 91 m_w \
    "DROP TABLE IF EXISTS f_wide_drop_packed;
     CREATE TABLE f_wide_drop_packed (
         id UInt64, v UInt64, w UInt64,
         INDEX m_v v TYPE minmax GRANULARITY 1,
         INDEX m_w w TYPE minmax GRANULARITY 1
     ) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', index_granularity = 1024;
     INSERT INTO f_wide_drop_packed SELECT number, number * 7, number * 11 FROM numbers(2000);"

# Scenario G: Wide, packed minmax + per-file set, DROP the per-file set. Per-file files gone,
# archive untouched; m_v still prunes. The set_index_layout line asserts s_s really is per-file
# before the drop (otherwise this would exercise dropping a packed index).
run_scenario "G_wide_drop_perfile_index" g_wide_drop_perfile \
    "ALTER TABLE g_wide_drop_perfile DROP INDEX s_s" \
    "v BETWEEN 70 AND 700" 91 m_v \
    "DROP TABLE IF EXISTS g_wide_drop_perfile;
     CREATE TABLE g_wide_drop_perfile ($MIXED_COLUMNS) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 4096, index_granularity = 1024;
     INSERT INTO g_wide_drop_perfile $MIXED_INSERT;" \
    "$PACKED_MIXED_FILES"

# Scenario H: Wide, packed minmax, ALTER ADD COLUMN (metadata only, no part rewrite).
# Same part, archive untouched.
run_scenario "H_wide_add_column" h_wide_add_column \
    "ALTER TABLE h_wide_add_column ADD COLUMN extra String DEFAULT ''" \
    "v BETWEEN 70 AND 700" 91 m_v \
    "DROP TABLE IF EXISTS h_wide_add_column;
     CREATE TABLE h_wide_add_column (
         id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
     ) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', index_granularity = 1024;
     INSERT INTO h_wide_add_column SELECT number, number * 7 FROM numbers(2000);"

# Scenario I: Compact, sole packed minmax, ALTER UPDATE on the indexed column. Compact mutations
# rewrite the whole part via MutateAllPartColumnsTask; verify the archive path is wired there too.
# min_bytes_for_wide_part / min_rows_for_wide_part are pinned high so the part stays Compact even
# when --random-merge-tree-settings injects min_bytes_for_wide_part = 0.
run_scenario "I_compact_update_indexed" i_compact_update_indexed \
    "ALTER TABLE i_compact_update_indexed UPDATE v = v + 1 WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v \
    "DROP TABLE IF EXISTS i_compact_update_indexed;
     CREATE TABLE i_compact_update_indexed (
         id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
     ) ENGINE = MergeTree ORDER BY id
     SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 100000000,
              packed_skip_index_max_bytes = '1M', index_granularity = 1024;
     INSERT INTO i_compact_update_indexed SELECT number, number * 7 FROM numbers(2000);"

# Scenario J: Wide, sole packed minmax, DROP the only packed index. Special-cased because it
# compares the secondary-index bytes before and after the DROP: the archive disappears entirely,
# so the bytes go from >0 to 0 and m_v becomes "missing" in the plan (query falls back to the PK).
out_J=$($CLIENT --multiquery $SYNC_ALTER -q "
    DROP TABLE IF EXISTS j_wide_drop_only;
    CREATE TABLE j_wide_drop_only (
        id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', index_granularity = 1024;
    INSERT INTO j_wide_drop_only SELECT number, number * 7 FROM numbers(2000);

    SELECT 'sec_before', secondary_indices_compressed_bytes FROM system.parts
        WHERE database = currentDatabase() AND table = 'j_wide_drop_only' AND active ORDER BY name LIMIT 1;
    ALTER TABLE j_wide_drop_only DROP INDEX m_v;
    SELECT 'sec_after', secondary_indices_compressed_bytes FROM system.parts
        WHERE database = currentDatabase() AND table = 'j_wide_drop_only' AND active ORDER BY name LIMIT 1;
    SELECT 'count', count() FROM j_wide_drop_only WHERE v BETWEEN 70 AND 700;
    SELECT '@check';
    CHECK TABLE j_wide_drop_only SETTINGS check_query_single_value_result = 1;
    SELECT '@explain';
    EXPLAIN indexes = 1 SELECT count() FROM j_wide_drop_only WHERE v BETWEEN 70 AND 700;
")
sec_before_J=$(tagged_value "$out_J" sec_before)
sec_after_J=$(tagged_value "$out_J" sec_after)
count_J=$(tagged_value "$out_J" count)
check_J=$(line_after_marker "$out_J" '@check')
granules_J=$(block_after_marker "$out_J" '@explain' | explain_index_status m_v)
echo "[J_wide_drop_only_packed_index]"
echo "  secondary_indices_bytes_before_positive=$( ((sec_before_J > 0)) && echo yes || echo no )"
echo "  secondary_indices_bytes_after=$sec_after_J"
echo "  count=$count_J expected=91 granules(m_v)=$granules_J check=$check_J"

$CLIENT -q "
    DROP TABLE IF EXISTS
        a_wide_indexed, b_wide_unindexed, c_wide_lwd, d_wide_mixed_update_set,
        e_wide_mixed_update_minmax, f_wide_drop_packed, g_wide_drop_perfile,
        h_wide_add_column, i_compact_update_indexed, j_wide_drop_only
"
