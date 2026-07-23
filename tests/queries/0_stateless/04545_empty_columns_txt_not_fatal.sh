#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-object-storage, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for a data-loss bug: writeColumns rewrites columns.txt in place (no atomic rename,
# no fsync), so an interrupted rewrite plus a power loss can leave a zero-byte columns.txt in a
# committed part directory. An empty columns.txt used to throw on load (NamesAndTypesList::readText
# begins with assertString) and detach the whole part as broken, losing every row of an
# otherwise-intact part. It must instead be treated like an absent columns.txt: for a wide part the
# column list (including any persistent virtual columns the part carries) is rebuilt from metadata.

# Each part's columns.txt is manipulated by an absolute path captured once, so every table must hold
# exactly one active part directory with no covered sibling. Stop merges before inserting so no merge
# produces a covering part, pin the block-size settings so a single insert yields one part regardless
# of CI randomization, and force wide parts. Assertions are server-side (row counts) except a direct
# read of columns.txt while the part is detached (quiescent, no server race) to prove persistence.

run_case()
{
    local table="$1"          # table name (already created, one active wide part)
    local expect_rows="$2"    # rows expected to survive the empty-columns.txt reload
    local expect_columns="$3" # declared columns expected in the rebuilt columns.txt

    local data_path
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(count() != 1, 'Expected exactly one active part in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active" > /dev/null || exit 1
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(part_type != 'Wide', 'Expected a Wide part in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1" > /dev/null || exit 1
    data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1")
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(substring('${data_path}', 1, 1) != '/', 'Path is relative: ${data_path}')" > /dev/null || exit 1

    echo "-- ${table}: rows before"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table}"

    # Empty (zero-byte) columns.txt must not brick the part.
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    : > "${data_path}columns.txt"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: rows after empty columns.txt reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table}"

    # Persistence: detach so the part is quiescent, read columns.txt from disk. It must now be
    # non-empty (a rebuild that only lived in memory would leave it empty and brick on this reload)
    # and list both declared columns `a` and `s` (a rebuild that dropped one would list fewer).
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    echo "-- ${table}: persisted columns.txt non-empty"
    [ -s "${data_path}columns.txt" ] && echo 1 || echo 0
    echo "-- ${table}: persisted columns.txt lists ${expect_columns} declared columns"
    grep -cE '^`(a|s)` ' "${data_path}columns.txt"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: rows after recovered reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table}"

    # Absent columns.txt must still self-heal (regression guard for the pre-existing path).
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    rm -f "${data_path}columns.txt"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: rows after absent columns.txt reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
}

# ---- Case A: plain wide part (only the declared physical columns) ----
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns (a UInt64, s String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns SELECT number, toString(number) FROM numbers(1000)"
run_case t_empty_columns 1000 2

# ---- Case B: wide part with persistent virtual columns _block_number and _block_offset ----
# The part physically carries these columns; the rebuild must include them (order: physical first,
# then persistent virtuals) or columns_substreams.txt validation detaches the part.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_bn"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_bn (a UInt64, s String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 1, enable_block_offset_column = 1;
"
# Two inserts + OPTIMIZE FINAL produce a merged part that physically writes _block_number/_block_offset.
# Stop merges only after the merge so the captured part directory does not move.
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_bn SELECT number, toString(number) FROM numbers(500)"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_bn SELECT number + 500, toString(number) FROM numbers(500)"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_empty_columns_bn FINAL"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_bn"
run_case t_empty_columns_bn 1000 2

# ---- Case C: wide part with a lightweight-delete mask (persistent virtual _row_exists) ----
# The recovery must keep _row_exists, otherwise the deletion mask is silently dropped and deleted
# rows reappear. Expect 500 live rows (half deleted) to survive the empty-columns.txt reload.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_ld"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_ld (a UInt64, s String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_ld SELECT number, toString(number) FROM numbers(1000)"
# Materialize the deletion mask, then stop merges so the mutated part directory does not move.
${CLICKHOUSE_CLIENT} --mutations_sync 2 --query "DELETE FROM t_empty_columns_ld WHERE a % 2 = 0"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_ld"
run_case t_empty_columns_ld 500 2

# A column whose type stores no <column>.bin stream (only named substreams) must not be dropped from
# the rebuilt list. A count()-only oracle cannot catch this: the row count survives, but the column
# is treated as missing on read and every value is silently synthesized as a default. Assert a digest
# of the column's real values, and check the persisted columns.txt still lists the column.
run_case_col()
{
    local table="$1"        # table name (already created, one active wide part)
    local col_expr="$2"     # expression digesting the multi-stream column's real values
    local col_name="$3"     # column name that must remain in columns.txt after recovery

    local data_path
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(count() != 1, 'Expected exactly one active part in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active" > /dev/null || exit 1
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(part_type != 'Wide', 'Expected a Wide part in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1" > /dev/null || exit 1
    data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1")
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(substring('${data_path}', 1, 1) != '/', 'Path is relative: ${data_path}')" > /dev/null || exit 1

    echo "-- ${table}: value digest before"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${col_expr} FROM ${table}"

    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    : > "${data_path}columns.txt"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: value digest after empty columns.txt reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${col_expr} FROM ${table}"

    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    echo "-- ${table}: recovered columns.txt lists ${col_name}"
    grep -q "\`${col_name}\`" "${data_path}columns.txt" && echo 1 || echo 0
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
}

# ---- Case D: wide part with a Tuple column (no <column>.bin stream, only named element streams) ----
# SerializationTuple emits only element streams, so getFileNameForColumn must enumerate the column's
# streams (not probe a single fixed path) or the whole Tuple is dropped and read back as defaults.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_tuple"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_tuple (a UInt64, t Tuple(x UInt64, y String))
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_tuple"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_tuple SELECT number, (number * 2, toString(number)) FROM numbers(1000)"
run_case_col t_empty_columns_tuple "sum(t.x)" "t"

# ---- Case E: wide part with a Map column (no <column>.bin stream, only size/key/value streams) ----
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_map"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_map (a UInt64, m Map(String, UInt64))
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_map"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_map SELECT number, map('k', number * 3) FROM numbers(1000)"
run_case_col t_empty_columns_map "sum(m['k'])" "m"

# ---- Case G: Map stored with bucketed serialization (streams differ from the default one) ----
# A bucketed Map writes m.buckets_info, m.0.size0, m.0.keys, ... none of which match the default
# serialization's streams (m.size0, m.keys, ...). Presence detection during recovery must not assume
# the default serialization, or the whole column is judged absent and dropped, and columns_substreams.txt
# validation then detaches the part. Pinned so the bucketed path is exercised on every run.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_map_bucketed"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_map_bucketed (a UInt64, m Map(String, UInt64))
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0,
             map_serialization_version = 'with_buckets',
             map_serialization_version_for_zero_level_parts = 'with_buckets',
             max_buckets_in_map = 11, map_buckets_strategy = 'constant';
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_map_bucketed"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_map_bucketed SELECT number, map('k', number * 3) FROM numbers(1000)"
run_case_col t_empty_columns_map_bucketed "sum(m['k'])" "m"

# ---- Case F: Nested sibling added by ALTER, present on disk only via shared offsets ----
# With share_nested_offsets a Nested column added by ALTER (n.b) has no data of its own; its only
# on-disk stream is the offsets stream owned by n.a. The rebuild must decide presence from a column's
# own streams (all of them), not from any stream that merely exists, or n.b is wrongly included and
# columns_substreams.txt validation detaches the part. n.a's data must survive; n.b stays absent.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_nested"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_nested (id UInt64, \`n.a\` Array(UInt64))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             share_nested_offsets = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_nested"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_nested SELECT number, [number, number + 1] FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_empty_columns_nested ADD COLUMN \`n.b\` Array(String)"
data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_columns_nested' AND active LIMIT 1")
echo "-- t_empty_columns_nested: n.a digest before"
${CLICKHOUSE_CLIENT} --query "SELECT sum(arraySum(n.a)) FROM t_empty_columns_nested"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_nested"
: > "${data_path}columns.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_nested" 2>/dev/null
echo "-- t_empty_columns_nested: n.a digest after empty columns.txt reload"
${CLICKHOUSE_CLIENT} --query "SELECT sum(arraySum(n.a)) FROM t_empty_columns_nested"
echo "-- t_empty_columns_nested: recovered columns.txt excludes data-less n.b"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_nested"
grep -q '`n.b`' "${data_path}columns.txt" && echo 1 || echo 0
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_nested" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_columns_nested"

# ---- Case H: recovery when columns_substreams.txt is also absent (legacy stream-enumeration path) ----
# columns_substreams.txt is the primary presence oracle, but a part predating it must still recover by
# enumerating each column's own streams. Remove both files so the fallback is exercised, on a Tuple
# (no <column>.bin, only element streams) that a single-fixed-path probe would wrongly drop.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_fallback"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_fallback (a UInt64, t Tuple(x UInt64, y String))
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_fallback"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_fallback SELECT number, (number * 2, toString(number)) FROM numbers(1000)"
data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_columns_fallback' AND active LIMIT 1")
echo "-- t_empty_columns_fallback: t.x digest before"
${CLICKHOUSE_CLIENT} --query "SELECT sum(t.x) FROM t_empty_columns_fallback"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_fallback"
: > "${data_path}columns.txt"
rm -f "${data_path}columns_substreams.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_fallback" 2>/dev/null
echo "-- t_empty_columns_fallback: t.x digest after empty columns.txt + absent columns_substreams.txt reload"
${CLICKHOUSE_CLIENT} --query "SELECT sum(t.x) FROM t_empty_columns_fallback"
echo "-- t_empty_columns_fallback: recovered columns.txt lists t"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_fallback"
grep -q '`t`' "${data_path}columns.txt" && echo 1 || echo 0
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_fallback" 2>/dev/null
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_columns_fallback"
