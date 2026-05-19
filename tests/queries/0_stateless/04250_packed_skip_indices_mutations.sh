#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: EXPLAIN output and active part lists differ across replicas.
#
# Mutation matrix for packed skip indices. Each scenario verifies state via system tables only
# (no on-disk file listing, so the assertions work for object-storage-backed deployments too):
#
#  - system.parts.secondary_indices_compressed_bytes reflects packed-archive contents (a packed
#    minmax index contributes >0; dropping it reduces or zeros the column).
#  - system.parts.bytes_on_disk and system.parts.files change consistently when the archive is
#    rebuilt smaller or removed entirely.
#  - EXPLAIN indexes=1 confirms the surviving skip indices still drive granule filtering.
#  - CHECK TABLE confirms checksums consistency after each mutation.
#
# Coverage matrix: Compact x Wide; sole-packed vs mixed (packed + per-file); ALTER UPDATE on
# indexed and non-indexed columns; lightweight DELETE; DROP INDEX of a packed index, of a
# per-file index, and of all packed indices; ALTER ADD COLUMN (metadata-only).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT"
SYNC_ALTER="--mutations_sync=2 --alter_sync=2"

part_metric() {
    # Usage: part_metric <table> <column> [<part_name>]
    # When <part_name> is omitted the active part is used.
    local table="$1"; local col="$2"; local part_filter=""
    if [[ -n "$3" ]]; then
        part_filter="AND name = '$3'"
    else
        part_filter="AND active"
    fi
    $CLIENT -q "
        SELECT $col FROM system.parts
        WHERE database = currentDatabase() AND table = '$table' $part_filter
        ORDER BY name LIMIT 1"
}

check_table() {
    $CLIENT -q "CHECK TABLE $1 SETTINGS check_query_single_value_result = 1"
}

# Granules kept/total for the given index according to EXPLAIN indexes=1. "missing" if the index
# isn't in the plan (which would mean it wasn't usable / wasn't loaded).
skip_granules() {
    local table="$1"; local index_name="$2"; local where="$3"
    $CLIENT -q "EXPLAIN indexes = 1 SELECT count() FROM $table WHERE $where" \
        | awk -v want="$index_name" '
            /^[[:space:]]*Name:/ {
                gsub(/.*Name: /, "", $0); gsub(/[[:space:]]+$/, "", $0)
                in_target = ($0 == want)
            }
            in_target && /Granules:/ { print $NF; found = 1; exit }
            END { if (!found) print "missing" }'
}

# Run a scenario and print labelled before/after state for system.parts metrics, plus the query
# result and CHECK TABLE.
#
# Usage: run_scenario <label> <table> <mutation_sql> <where> <expected_count> <granules_index>
run_scenario() {
    local label="$1"; local table="$2"; local mutation="$3"; local where="$4"; local expected="$5"; local idx="$6"
    local sec_before sec_after bytes_before bytes_after files_before files_after

    sec_before=$(part_metric "$table" "secondary_indices_compressed_bytes")
    bytes_before=$(part_metric "$table" "bytes_on_disk")
    files_before=$(part_metric "$table" "files")

    $CLIENT $SYNC_ALTER -q "$mutation"

    sec_after=$(part_metric "$table" "secondary_indices_compressed_bytes")
    bytes_after=$(part_metric "$table" "bytes_on_disk")
    files_after=$(part_metric "$table" "files")

    local count granules check
    count=$($CLIENT -q "SELECT count() FROM $table WHERE $where")
    granules=$(skip_granules "$table" "$idx" "$where")
    check=$(check_table "$table")

    # Comparators emit text labels so the reference is robust against absolute byte sizes.
    local sec_cmp
    if [[ "$sec_before" == "$sec_after" ]]; then sec_cmp="eq"
    elif (( sec_after < sec_before )); then sec_cmp="lt"
    elif (( sec_after > sec_before )); then sec_cmp="gt"
    fi
    local bytes_cmp
    if [[ "$bytes_before" == "$bytes_after" ]]; then bytes_cmp="eq"
    elif (( bytes_after < bytes_before )); then bytes_cmp="lt"
    elif (( bytes_after > bytes_before )); then bytes_cmp="gt"
    fi

    echo "[$label]"
    echo "  secondary_indices_bytes: $sec_cmp  bytes_on_disk: $bytes_cmp  files_delta=$((files_after - files_before))"
    echo "  count=$count expected=$expected granules($idx)=$granules check=$check"
}

# ----- Scenario A: Wide, sole packed minmax, ALTER UPDATE on the indexed column.
# Expect: archive rebuilt by the writer; secondary_indices_bytes stays > 0 (the minmax got
# recomputed). bytes_on_disk shouldn't grow.
$CLIENT -q "DROP TABLE IF EXISTS a_wide_indexed"
$CLIENT -q "
    CREATE TABLE a_wide_indexed (
        id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO a_wide_indexed SELECT number, number * 7 FROM numbers(10000)"
run_scenario "A_wide_update_indexed" a_wide_indexed \
    "ALTER TABLE a_wide_indexed UPDATE v = v + 1 WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v

# ----- Scenario B: Wide, sole packed minmax, ALTER UPDATE on a NON-indexed column.
# Expect: archive hardlinked (same bytes/files), m_v still works.
$CLIENT -q "DROP TABLE IF EXISTS b_wide_unindexed"
$CLIENT -q "
    CREATE TABLE b_wide_unindexed (
        id UInt64, v UInt64, s String, INDEX m_v v TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO b_wide_unindexed SELECT number, number * 7, toString(number % 50) FROM numbers(10000)"
run_scenario "B_wide_update_unindexed" b_wide_unindexed \
    "ALTER TABLE b_wide_unindexed UPDATE s = concat(s, '_x') WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v

# ----- Scenario C: Wide, packed minmax, lightweight DELETE. Index data untouched.
$CLIENT -q "DROP TABLE IF EXISTS c_wide_lwd"
$CLIENT -q "
    CREATE TABLE c_wide_lwd (
        id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO c_wide_lwd SELECT number, number * 7 FROM numbers(10000)"
run_scenario "C_wide_lightweight_delete" c_wide_lwd \
    "DELETE FROM c_wide_lwd WHERE id < 100" \
    "v BETWEEN 70 AND 700" 1 m_v

# ----- Scenario D: Wide, mixed (packed minmax + per-file set), ALTER UPDATE on SET's column.
# Expect: per-file set rebuilt, archive untouched, m_v still works.
$CLIENT -q "DROP TABLE IF EXISTS d_wide_mixed_update_set"
$CLIENT -q "
    CREATE TABLE d_wide_mixed_update_set (
        id UInt64, v UInt64, s String,
        INDEX m_v v TYPE minmax GRANULARITY 1,
        INDEX s_s s TYPE set(100) GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO d_wide_mixed_update_set SELECT number, number * 7, toString(number % 50) FROM numbers(10000)"
run_scenario "D_wide_mixed_update_set" d_wide_mixed_update_set \
    "ALTER TABLE d_wide_mixed_update_set UPDATE s = concat(s, '_x') WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v

# ----- Scenario E: Wide mixed, ALTER UPDATE on minmax's column. Archive recomputed, set kept.
$CLIENT -q "DROP TABLE IF EXISTS e_wide_mixed_update_minmax"
$CLIENT -q "
    CREATE TABLE e_wide_mixed_update_minmax (
        id UInt64, v UInt64, s String,
        INDEX m_v v TYPE minmax GRANULARITY 1,
        INDEX s_s s TYPE set(100) GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO e_wide_mixed_update_minmax SELECT number, number * 7, toString(number % 50) FROM numbers(10000)"
run_scenario "E_wide_mixed_update_minmax" e_wide_mixed_update_minmax \
    "ALTER TABLE e_wide_mixed_update_minmax UPDATE v = v + 1 WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v

# ----- Scenario F: Wide, two packed minmax indices, DROP one.
# Expect: archive rebuilt smaller via the in-place filter; m_w still filters.
# secondary_indices_bytes should drop (one of two packed indices removed).
$CLIENT -q "DROP TABLE IF EXISTS f_wide_drop_packed"
$CLIENT -q "
    CREATE TABLE f_wide_drop_packed (
        id UInt64, v UInt64, w UInt64,
        INDEX m_v v TYPE minmax GRANULARITY 1,
        INDEX m_w w TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO f_wide_drop_packed SELECT number, number * 7, number * 11 FROM numbers(10000)"
run_scenario "F_wide_drop_one_of_two_packed" f_wide_drop_packed \
    "ALTER TABLE f_wide_drop_packed DROP INDEX m_v" \
    "w BETWEEN 110 AND 1100" 91 m_w

# ----- Scenario G: Wide, packed minmax + per-file set, DROP the per-file set.
# Expect: per-file set files gone, archive untouched (same size); m_v still works.
$CLIENT -q "DROP TABLE IF EXISTS g_wide_drop_perfile"
$CLIENT -q "
    CREATE TABLE g_wide_drop_perfile (
        id UInt64, v UInt64, s String,
        INDEX m_v v TYPE minmax GRANULARITY 1,
        INDEX s_s s TYPE set(100) GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO g_wide_drop_perfile SELECT number, number * 7, toString(number % 50) FROM numbers(10000)"
run_scenario "G_wide_drop_perfile_index" g_wide_drop_perfile \
    "ALTER TABLE g_wide_drop_perfile DROP INDEX s_s" \
    "v BETWEEN 70 AND 700" 91 m_v

# ----- Scenario H: Wide, packed minmax, ALTER ADD COLUMN (metadata only, no part rewrite).
# Expect: same part, archive untouched.
$CLIENT -q "DROP TABLE IF EXISTS h_wide_add_column"
$CLIENT -q "
    CREATE TABLE h_wide_add_column (
        id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO h_wide_add_column SELECT number, number * 7 FROM numbers(10000)"
run_scenario "H_wide_add_column" h_wide_add_column \
    "ALTER TABLE h_wide_add_column ADD COLUMN extra String DEFAULT ''" \
    "v BETWEEN 70 AND 700" 91 m_v

# ----- Scenario I: Compact, sole packed minmax, ALTER UPDATE on indexed column.
# Compact mutations rewrite the whole part via MutateAllPartColumnsTask; verify the archive
# code path is wired in that branch too.
$CLIENT -q "DROP TABLE IF EXISTS i_compact_update_indexed"
$CLIENT -q "
    CREATE TABLE i_compact_update_indexed (
        id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO i_compact_update_indexed SELECT number, number * 7 FROM numbers(10000)"
run_scenario "I_compact_update_indexed" i_compact_update_indexed \
    "ALTER TABLE i_compact_update_indexed UPDATE v = v + 1 WHERE id < 100" \
    "v BETWEEN 70 AND 700" 91 m_v

# ----- Scenario J: Wide, sole packed minmax, DROP the only packed index.
# Expect: archive disappears entirely; secondary_indices_compressed_bytes becomes 0;
# files count decreases by one. The query falls back to the primary key (m_v shows up as
# "missing" in EXPLAIN).
$CLIENT -q "DROP TABLE IF EXISTS j_wide_drop_only"
$CLIENT -q "
    CREATE TABLE j_wide_drop_only (
        id UInt64, v UInt64, INDEX m_v v TYPE minmax GRANULARITY 1
    ) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_types = 'minmax', index_granularity = 1024"
$CLIENT -q "INSERT INTO j_wide_drop_only SELECT number, number * 7 FROM numbers(10000)"
sec_before_J=$(part_metric j_wide_drop_only secondary_indices_compressed_bytes)
files_before_J=$(part_metric j_wide_drop_only files)
$CLIENT $SYNC_ALTER -q "ALTER TABLE j_wide_drop_only DROP INDEX m_v"
sec_after_J=$(part_metric j_wide_drop_only secondary_indices_compressed_bytes)
files_after_J=$(part_metric j_wide_drop_only files)
granules_J=$(skip_granules j_wide_drop_only m_v "v BETWEEN 70 AND 700")
count_J=$($CLIENT -q "SELECT count() FROM j_wide_drop_only WHERE v BETWEEN 70 AND 700")
echo "[J_wide_drop_only_packed_index]"
echo "  secondary_indices_bytes_before_positive=$([[ $sec_before_J -gt 0 ]] && echo yes || echo no)"
echo "  secondary_indices_bytes_after=$sec_after_J"
echo "  files_delta=$((files_after_J - files_before_J))"
echo "  count=$count_J expected=91 granules(m_v)=$granules_J check=$(check_table j_wide_drop_only)"

for t in a_wide_indexed b_wide_unindexed c_wide_lwd d_wide_mixed_update_set \
         e_wide_mixed_update_minmax f_wide_drop_packed g_wide_drop_perfile \
         h_wide_add_column i_compact_update_indexed j_wide_drop_only; do
    $CLIENT -q "DROP TABLE IF EXISTS $t"
done
