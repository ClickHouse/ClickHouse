#!/usr/bin/env bash
# Tags: long, no-fasttest, no-shared-merge-tree, no-object-storage, no-parallel-replicas

# Regression test for a data-loss bug: writeColumns rewrites columns.txt in place (no atomic rename,
# no fsync), so an interrupted rewrite plus a power loss can leave a zero-byte columns.txt in a
# committed part directory. An empty columns.txt used to throw on load (NamesAndTypesList::readText
# begins with assertString) and detach the whole part as broken, losing every row of an
# otherwise-intact part. It must instead be treated like an absent columns.txt: for a wide part the
# column list (including any persistent virtual columns the part carries) is rebuilt from metadata.

# Each part's columns.txt is manipulated by an absolute path captured once, so every table must hold
# exactly one active part directory with no covered sibling. Stop merges before inserting so no merge
# produces a covering part, pin the block-size settings so a single insert yields one part regardless
# of CI randomization, and force wide parts.

# Assertions are made server-side (row counts, per-column digests) rather than by stat-ing the
# on-disk columns.txt: an external stat/grep of a live part directory races the server's own
# reads/writes and cache prewarming under CI randomization. Reloading the part from disk
# (DETACH/ATTACH) forces the recovered columns.txt to be read back from disk, so a reload that
# returns all rows proves recovery persisted a non-empty columns.txt, and a per-column digest that
# matches after reload proves no column was dropped (a dropped column would read back as defaults).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

run_case()
{
    local table="$1"          # table name (already created, one active wide part)
    local expect_rows="$2"    # rows expected to survive the empty-columns.txt reload

    local data_path
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(count() != 1, 'Expected exactly one active part in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active" > /dev/null || exit 1
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(part_type != 'Wide', 'Expected a Wide part in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1" > /dev/null || exit 1
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(part_storage_type != 'Full', 'Expected Full part storage in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1" > /dev/null || exit 1
    data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1")
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(substring('${data_path}', 1, 1) != '/', 'Path is relative: ${data_path}')" > /dev/null || exit 1

    echo "-- ${table}: rows before"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a), sum(cityHash64(s)) FROM ${table}"

    # Empty (zero-byte) columns.txt must not brick the part.
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    : > "${data_path}columns.txt"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: rows after empty columns.txt reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a), sum(cityHash64(s)) FROM ${table}"

    # Persistence proof (server-side, no stat of the live part): reload the part from disk. A rebuild
    # that only lived in memory would leave columns.txt empty and brick on this reload, so a reload
    # that still returns the same digest proves recovery persisted a valid columns.txt to disk. The
    # per-column digest (not just count()) also catches a recovery that keeps the row count but drops
    # a physical column, which would then read back as all-default values.
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    # The part is quiescent here, so the file on disk is the one recovery wrote: a rebuild that never
    # reached disk would still be zero bytes and re-run recovery on every load.
    echo "-- ${table}: rebuilt columns.txt on disk"
    awk 'NR == 1 { print } /^`/ { print }' "${data_path}columns.txt"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: rows after recovered reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a), sum(cityHash64(s)) FROM ${table}"

    # Absent columns.txt must still self-heal (regression guard for the pre-existing path).
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    rm -f "${data_path}columns.txt"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: rows after absent columns.txt reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a), sum(cityHash64(s)) FROM ${table}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
}

# ---- Case A: plain wide part (only the declared physical columns) ----
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns (a UInt64, s String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns SELECT number, toString(number) FROM numbers(1000)"
run_case t_empty_columns 1000

# ---- Case B: wide part with persistent virtual columns _block_number and _block_offset ----
# The part physically carries these columns; the rebuild must include them (order: physical first,
# then persistent virtuals) or columns_substreams.txt validation detaches the part.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_bn"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_bn (a UInt64, s String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 1, enable_block_offset_column = 1;
"
# Two inserts + OPTIMIZE FINAL produce a merged part that physically writes _block_number/_block_offset.
# Stop merges only after the merge so the captured part directory does not move.
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_bn SELECT number, toString(number) FROM numbers(500)"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_bn SELECT number + 500, toString(number) FROM numbers(500)"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_empty_columns_bn FINAL"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_bn"
run_case t_empty_columns_bn 1000

# ---- Case C: wide part with a lightweight-delete mask (persistent virtual _row_exists) ----
# The recovery must keep _row_exists, otherwise the deletion mask is silently dropped and deleted
# rows reappear. Expect 500 live rows (half deleted) to survive the empty-columns.txt reload.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_ld"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_ld (a UInt64, s String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_ld SELECT number, toString(number) FROM numbers(1000)"
# Materialize the deletion mask, then stop merges so the mutated part directory does not move.
${CLICKHOUSE_CLIENT} --mutations_sync 2 --query "DELETE FROM t_empty_columns_ld WHERE a % 2 = 0"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_ld"
run_case t_empty_columns_ld 500

# A column whose type stores no <column>.bin stream (only named substreams) must not be dropped from
# the rebuilt list. A count()-only oracle cannot catch this: the row count survives, but the column
# is treated as missing on read and every value is silently synthesized as a default. Assert a digest
# of the column's real values, both right after the empty-columns.txt recovery and again after a
# fresh reload from disk (which proves the rebuilt columns.txt was persisted, not just recovered in
# memory) -- server-side, without stat-ing the live part file (which races the server).
run_case_col()
{
    local table="$1"        # table name (already created, one active wide part)
    local col_expr="$2"     # expression digesting the multi-stream column's real values

    local data_path
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(count() != 1, 'Expected exactly one active part in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active" > /dev/null || exit 1
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(part_type != 'Wide', 'Expected a Wide part in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1" > /dev/null || exit 1
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(part_storage_type != 'Full', 'Expected Full part storage in ${table}') FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1" > /dev/null || exit 1
    data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active LIMIT 1")
    ${CLICKHOUSE_CLIENT} --query "SELECT throwIf(substring('${data_path}', 1, 1) != '/', 'Path is relative: ${data_path}')" > /dev/null || exit 1

    echo "-- ${table}: value digest before"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${col_expr} FROM ${table}"

    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    : > "${data_path}columns.txt"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: value digest after empty columns.txt reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${col_expr} FROM ${table}"

    # Persistence proof: reload from disk and re-digest. If the rebuilt columns.txt (with the
    # multi-stream column) was not persisted, this reload would drop the column and change the digest.
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}"
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${table}" 2>/dev/null
    echo "-- ${table}: value digest after recovered reload"
    ${CLICKHOUSE_CLIENT} --query "SELECT ${col_expr} FROM ${table}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
}

# ---- Case D: wide part with a Tuple column (no <column>.bin stream, only named element streams) ----
# SerializationTuple emits only element streams, so getFirstFileNameForColumn must enumerate the column's
# streams (not probe a single fixed path) or the whole Tuple is dropped and read back as defaults.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_tuple"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_tuple (a UInt64, t Tuple(x UInt64, y String))
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_tuple"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_tuple SELECT number, (number * 2, toString(number)) FROM numbers(1000)"
run_case_col t_empty_columns_tuple "sum(t.x)"

# ---- Case E: wide part with a Map column (no <column>.bin stream, only size/key/value streams) ----
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_map"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_map (a UInt64, m Map(String, UInt64))
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_map"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_map SELECT number, map('k', number * 3) FROM numbers(1000)"
run_case_col t_empty_columns_map "sum(m['k'])"

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
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 0, enable_block_offset_column = 0,
             map_serialization_version = 'with_buckets',
             map_serialization_version_for_zero_level_parts = 'with_buckets',
             max_buckets_in_map = 11, map_buckets_strategy = 'constant';
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_map_bucketed"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_map_bucketed SELECT number, map('k', number * 3) FROM numbers(1000)"
run_case_col t_empty_columns_map_bucketed "sum(m['k'])"

# ---- Case F: Nested sibling added by ALTER, present on disk only via shared offsets ----
# With share_nested_offsets a Nested column added by ALTER (n.b) has no data of its own; its only
# on-disk stream is the offsets stream owned by n.a. The rebuild must decide presence from a column's
# own streams (all of them), not from any stream that merely exists, or n.b is wrongly included and
# columns_substreams.txt validation detaches the part. Proven server-side: after empty-columns.txt
# recovery the part must stay ACTIVE (validation passed => n.b was not wrongly included) and n.a's
# data must survive a reload from disk.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_nested"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_nested (id UInt64, \`n.a\` Array(UInt64))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
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
# The part must have recovered as a single ACTIVE part (not detached-as-broken): a rebuild that
# wrongly included the data-less n.b would fail columns_substreams.txt validation and detach it.
echo "-- t_empty_columns_nested: one active part after recovery"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_columns_nested' AND active"
echo "-- t_empty_columns_nested: n.a digest after empty columns.txt reload"
${CLICKHOUSE_CLIENT} --query "SELECT sum(arraySum(n.a)) FROM t_empty_columns_nested"
# Persistence proof: reload from disk and re-digest n.a.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_nested"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_nested" 2>/dev/null
echo "-- t_empty_columns_nested: n.a digest after recovered reload"
${CLICKHOUSE_CLIENT} --query "SELECT sum(arraySum(n.a)) FROM t_empty_columns_nested"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_columns_nested"

# ---- Case I: columns_substreams.txt present but discarded as corrupted ----
# Recovery must refuse instead of inferring presence from the default serialization: a bucketed Map is
# stored as m.buckets_info, m.0.keys, ... so the default streams (m.size0, m.keys) are absent, the
# column would be judged missing, and writeColumns would persist that omission -- leaving an intact
# column reading back as all-default values. Refusing detaches the part, which is recoverable.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_discarded"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_discarded (a UInt64, m Map(String, UInt64))
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 0, enable_block_offset_column = 0,
             map_serialization_version = 'with_buckets',
             map_serialization_version_for_zero_level_parts = 'with_buckets',
             max_buckets_in_map = 11, map_buckets_strategy = 'constant';
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_discarded"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_discarded SELECT number, map('k', number * 3) FROM numbers(1000)"
data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_columns_discarded' AND active LIMIT 1")
echo "-- t_empty_columns_discarded: m digest before"
${CLICKHOUSE_CLIENT} --query "SELECT sum(m['k']) FROM t_empty_columns_discarded"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_discarded"
# Give the first substream a prefix that does not match its column: the rename-bug corruption that
# loadColumnsSubstreams discards for Wide parts. Rewriting whichever substream comes first keeps this
# independent of the stream names the randomized serialization versions produce.
sed -i '0,/^\t/s/^\t.*/\tnot_a_valid_prefix/' "${data_path}columns_substreams.txt"
: > "${data_path}columns.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_discarded" 2>/dev/null
echo "-- t_empty_columns_discarded: active parts after refused recovery"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_columns_discarded' AND active"
echo "-- t_empty_columns_discarded: part kept for recovery"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_empty_columns_discarded'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_columns_discarded"

# ---- Case H: recovery when columns_substreams.txt is also absent (legacy stream-enumeration path) ----
# columns_substreams.txt is the primary presence oracle, but a part predating it must still recover by
# enumerating each column's own streams. Remove both files so the fallback is exercised, on a Tuple
# (no <column>.bin, only element streams) that a single-fixed-path probe would wrongly drop. Proven
# server-side by the Tuple digest surviving both the recovery and a subsequent reload from disk.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_fallback"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_fallback (a UInt64, t Tuple(x UInt64, y String))
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
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
# Persistence proof: reload from disk and re-digest.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_fallback"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_fallback" 2>/dev/null
echo "-- t_empty_columns_fallback: t.x digest after recovered reload"
${CLICKHOUSE_CLIENT} --query "SELECT sum(t.x) FROM t_empty_columns_fallback"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_columns_fallback"

# ---- Case J: shared Nested offsets must not witness a data-less ALTER-added sibling ----
# On the legacy no-substreams path a column's presence is decided by enumerating its own streams. With
# share_nested_offsets the offsets stream is named after the Nested table, so it exists as soon as any
# sibling has data: accepting it lists a data-less n.b in the rebuilt columns.txt and CHECK TABLE then
# reports NO_FILE_IN_DATA_PART. n.b written with data is the control that presence is still detected.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_shared_offsets"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_shared_offsets (id UInt64, \`n.a\` Array(UInt64))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             share_nested_offsets = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_shared_offsets"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_shared_offsets SELECT number, [number, number + 1] FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_empty_columns_shared_offsets ADD COLUMN \`n.b\` Array(String)"
data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_columns_shared_offsets' AND active LIMIT 1")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_shared_offsets"
: > "${data_path}columns.txt"
rm -f "${data_path}columns_substreams.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_shared_offsets" 2>/dev/null
echo "-- t_empty_columns_shared_offsets: n.a digest after recovery"
${CLICKHOUSE_CLIENT} --query "SELECT sum(arraySum(n.a)) FROM t_empty_columns_shared_offsets"
echo "-- t_empty_columns_shared_offsets: consistent part after recovery"
${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_empty_columns_shared_offsets SETTINGS check_query_single_value_result = 1"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_columns_shared_offsets"

# Control: the same shape with n.b written with data must keep n.b, so the rule above rejects only
# streams the column does not own.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_shared_offsets_data"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_shared_offsets_data (id UInt64, \`n.a\` Array(UInt64), \`n.b\` Array(String))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             share_nested_offsets = 1,
             enable_block_number_column = 0, enable_block_offset_column = 0;
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_shared_offsets_data"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_shared_offsets_data SELECT number, [number, number + 1], ['x', 'y'] FROM numbers(1000)"
data_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_empty_columns_shared_offsets_data' AND active LIMIT 1")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_shared_offsets_data"
: > "${data_path}columns.txt"
rm -f "${data_path}columns_substreams.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_shared_offsets_data" 2>/dev/null
echo "-- t_empty_columns_shared_offsets_data: n.a and n.b digests after recovery"
${CLICKHOUSE_CLIENT} --query "SELECT sum(arraySum(n.a)), sum(length(n.b)) FROM t_empty_columns_shared_offsets_data"
echo "-- t_empty_columns_shared_offsets_data: consistent part after recovery"
${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_empty_columns_shared_offsets_data SETTINGS check_query_single_value_result = 1"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_columns_shared_offsets_data"

# ---- Case K: recovering a projection part must not list a stored virtual twice ----
# A projection lists the parent virtuals it stores among its own physical columns, so a rebuild that
# appends the persistent virtuals unconditionally writes `_block_number` twice; the projection then
# fails to load with DUPLICATE_COLUMN and is marked broken.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_empty_columns_projection"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_empty_columns_projection (id UInt64, v UInt64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 1, enable_block_offset_column = 1,
             allow_commit_order_projection = 1, deduplicate_merge_projection_mode = 'rebuild';
"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_empty_columns_projection ADD PROJECTION p (SELECT id, v, _block_number ORDER BY v)"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_projection SELECT number, number * 2 FROM numbers(300)"
${CLICKHOUSE_CLIENT} --max_insert_threads 1 --min_insert_block_size_rows 100000 --min_insert_block_size_bytes 0 --max_block_size 100000 --query "INSERT INTO t_empty_columns_projection SELECT number + 300, number FROM numbers(300)"
# Merge so the projection is materialized in one part, then freeze the layout.
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_empty_columns_projection FINAL"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_empty_columns_projection"
proj_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_empty_columns_projection' AND active LIMIT 1")
${CLICKHOUSE_CLIENT} --query "SELECT throwIf(substring('${proj_path}', 1, 1) != '/', 'Projection path is relative: ${proj_path}')" > /dev/null || exit 1
echo "-- t_empty_columns_projection: rows before"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_empty_columns_projection"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_empty_columns_projection"
: > "${proj_path}columns.txt"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_empty_columns_projection" 2>/dev/null
echo "-- t_empty_columns_projection: usable projection after recovery"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_empty_columns_projection' AND active AND NOT is_broken"
echo "-- t_empty_columns_projection: _block_number listed once in the rebuilt columns.txt"
grep -c '^`_block_number`' "${proj_path}columns.txt"
echo "-- t_empty_columns_projection: rows after recovery"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_empty_columns_projection"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_empty_columns_projection"
