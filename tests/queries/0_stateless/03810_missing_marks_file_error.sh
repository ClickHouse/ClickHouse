#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree, no-object-storage
# no-parallel: issues SYSTEM DROP MARK CACHE (server-wide shared state)
# no-shared-merge-tree, no-object-storage: the test removes a marks file from the local filesystem

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# When a column's marks file is listed in a part's checksums but is missing on disk
# (e.g. due to a rare concurrent race), reading that column must fail with a clear,
# typed error (NO_FILE_IN_DATA_PART) naming the part and the file, not an opaque
# std::filesystem "in file_size: No such file" error.

# --- Case 1: query read path (MergeTreeMarksLoader::loadMarksImpl) ---

# ratio_of_defaults_for_sparse_serialization = 1: keep dense per-column marks. With sparse
# serialization a column gains a .sparse.idx substream and the missing main marks file is caught
# by the checksum consistency check (a different, already-typed error) instead of the marks loader.
$CLICKHOUSE_CLIENT -q "drop table if exists t_missing_marks sync;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_missing_marks (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, replace_long_file_name_to_hash = 0, prewarm_mark_cache = 0, ratio_of_defaults_for_sparse_serialization = 1"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_missing_marks SELECT number, number FROM numbers(1000)"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database=currentDatabase() and table='t_missing_marks' and active=1 limit 1")
# ensure path is absolute before touching the filesystem
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" > /dev/null || exit 1

# remove the marks file for column b. Wide marks are adaptive-compressed (.cmrk2), adaptive
# (.mrk2), non-adaptive-compressed (.cmrk) or non-adaptive (.mrk) depending on randomized
# index_granularity_bytes / compress_marks; cover every wide extension.
mark_file=""
for ext in cmrk2 mrk2 cmrk mrk; do
    if [ -f "${path}b.${ext}" ]; then
        mark_file="${path}b.${ext}"
        break
    fi
done
if [ -z "$mark_file" ]; then
    echo "NO MARKS FILE FOUND for column b in $path" >&2
    ls "$path" >&2
    exit 1
fi
rm -f "$mark_file"

# drop the mark cache so the marks are loaded from disk (and not served from cache)
$CLICKHOUSE_CLIENT -q "system drop mark cache"

# reading column b must now raise the typed error mentioning the missing marks file.
# column b's marks are listed in the part's checksums, so the message must say "listed".
echo "--- query read path ---"
err=$($CLICKHOUSE_CLIENT -q "select sum(b) from t_missing_marks" 2>&1)
# emit each expected phrase in a fixed order; do not pipe through `sort` (its collation is locale-dependent).
echo "$err" | grep -qF "NO_FILE_IN_DATA_PART" && echo "NO_FILE_IN_DATA_PART"
echo "$err" | grep -qF "does not exist on disk in part" && echo "does not exist on disk in part"
echo "$err" | grep -qF "is listed in the part's checksums" && echo "is listed in the part's checksums"

# send_logs_level=none: with the marks file removed, dropping the table logs a CANNOT_UNLINK <Error>
# during directory cleanup under the Ordinary engine, which would otherwise reach client stderr.
$CLICKHOUSE_CLIENT --send_logs_level=none -q "drop table t_missing_marks sync;"

# --- Case 2: index-granularity load path (MergeTreeDataPartWide::loadIndexGranularity) ---
# Index granularity is loaded directly from the first column's marks file when a part is
# loaded, before any query reaches MergeTreeMarksLoader. Removing that marks file and
# reloading the table must produce the same typed diagnostic, surfaced via the broken-part
# handling that runs on load.

$CLICKHOUSE_CLIENT -q "drop table if exists t_missing_granularity_marks sync;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_missing_granularity_marks (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, replace_long_file_name_to_hash = 0, prewarm_mark_cache = 0, ratio_of_defaults_for_sparse_serialization = 1"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_missing_granularity_marks SELECT number, number FROM numbers(1000)"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database=currentDatabase() and table='t_missing_granularity_marks' and active=1 limit 1")
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" > /dev/null || exit 1

# detach the table so the part is unloaded, then remove the first column's (a) marks file:
# that is the one loadIndexGranularity() reads.
$CLICKHOUSE_CLIENT -q "detach table t_missing_granularity_marks"

mark_file=""
for ext in cmrk2 mrk2 cmrk mrk; do
    if [ -f "${path}a.${ext}" ]; then
        mark_file="${path}a.${ext}"
        break
    fi
done
if [ -z "$mark_file" ]; then
    echo "NO MARKS FILE FOUND for column a in $path" >&2
    ls "$path" >&2
    exit 1
fi
rm -f "$mark_file"

# reloading the table re-reads index granularity from disk; the missing marks file makes the
# part broken-on-start instead of crashing with an opaque std::filesystem error.
# send_logs_level=none keeps the (expected) broken-part error out of the client stderr stream.
$CLICKHOUSE_CLIENT --send_logs_level=none -q "attach table t_missing_granularity_marks"

echo "--- index granularity load path ---"
$CLICKHOUSE_CLIENT -q "SELECT reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_missing_granularity_marks'"

# the broken-part handler logs our typed diagnostic; confirm the enriched message reached the log
# (without the fix this path logs only the opaque, path-only error). Anchor the logger on the
# current database (the storage logger is 'db.table (uuid)' under Atomic, plain 'db.table' under
# Ordinary): this matches both engines and excludes a same-named table from another test's database
# in the server-wide text_log. Pin the read settings: system.text_log can hold many rows under
# load, so randomized read limits / parallel-read / projection / query-condition-cache settings
# must not skew this scan.
$CLICKHOUSE_CLIENT -q "system flush logs text_log"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.text_log
WHERE event_date >= yesterday()
  AND event_time > now() - INTERVAL 5 MINUTE
  AND logger_name LIKE '%' || currentDatabase() || '.t_missing_granularity_marks%'
  AND message LIKE '%does not exist on disk in part%'
  AND message LIKE '%listed in the part%checksums%'
SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0, max_threads = 1, use_query_condition_cache = 0, optimize_use_implicit_projections = 0"

$CLICKHOUSE_CLIENT -q "drop table t_missing_granularity_marks sync;"

# --- Case 3: index-granularity load path for compact parts (MergeTreeDataPartCompact::loadIndexGranularity) ---
# A compact part keeps every column's marks in a single "data" marks file, read directly on part
# load through a different API (readFileIfExists) than the wide path. Removing that file and
# reloading the table must produce the same typed diagnostic, not the opaque std::filesystem error.

# large min_*_for_wide_part forces a compact part (the 1000-row insert stays below both thresholds).
$CLICKHOUSE_CLIENT -q "drop table if exists t_missing_compact_marks sync;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_missing_compact_marks (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, replace_long_file_name_to_hash = 0, prewarm_mark_cache = 0, ratio_of_defaults_for_sparse_serialization = 1"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_missing_compact_marks SELECT number, number FROM numbers(1000)"

# guard: the compact loader is only exercised if the part really is compact.
$CLICKHOUSE_CLIENT -q "select throwIf(part_type != 'Compact', 'Part is not compact') from system.parts where database=currentDatabase() and table='t_missing_compact_marks' and active=1 limit 1" > /dev/null || exit 1

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database=currentDatabase() and table='t_missing_compact_marks' and active=1 limit 1")
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" > /dev/null || exit 1

$CLICKHOUSE_CLIENT -q "detach table t_missing_compact_marks"

# the single compact marks file is "data" with extension .cmrk4 / .mrk4 (with substreams) or
# .cmrk3 / .mrk3 depending on randomized compress_marks; cover every compact extension.
mark_file=""
for ext in cmrk4 mrk4 cmrk3 mrk3; do
    if [ -f "${path}data.${ext}" ]; then
        mark_file="${path}data.${ext}"
        break
    fi
done
if [ -z "$mark_file" ]; then
    echo "NO MARKS FILE FOUND for compact data in $path" >&2
    ls "$path" >&2
    exit 1
fi
rm -f "$mark_file"

$CLICKHOUSE_CLIENT --send_logs_level=none -q "attach table t_missing_compact_marks"

echo "--- compact index granularity load path ---"
$CLICKHOUSE_CLIENT -q "SELECT reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_missing_compact_marks'"

$CLICKHOUSE_CLIENT -q "system flush logs text_log"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.text_log
WHERE event_date >= yesterday()
  AND event_time > now() - INTERVAL 5 MINUTE
  AND logger_name LIKE '%' || currentDatabase() || '.t_missing_compact_marks%'
  AND message LIKE '%does not exist on disk in part%'
  AND message LIKE '%listed in the part%checksums%'
SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0, max_threads = 1, use_query_condition_cache = 0, optimize_use_implicit_projections = 0"

$CLICKHOUSE_CLIENT -q "drop table t_missing_compact_marks sync;"
