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

$CLICKHOUSE_CLIENT -q "drop table if exists t_missing_marks sync;"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_missing_marks (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, replace_long_file_name_to_hash = 0, prewarm_mark_cache = 0"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_missing_marks SELECT number, number FROM numbers(1000)"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database=currentDatabase() and table='t_missing_marks' and active=1 limit 1")
# ensure path is absolute before touching the filesystem
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" > /dev/null || exit 1

# remove the marks file for column b (compressed wide marks use .cmrk2; fall back to .mrk2)
mark_file=""
for ext in cmrk2 mrk2 mrk; do
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
$CLICKHOUSE_CLIENT -q "select sum(b) from t_missing_marks" 2>&1 | grep -oF -e "NO_FILE_IN_DATA_PART" -e "does not exist on disk in part" -e "is listed in the part's checksums" | sort -u

$CLICKHOUSE_CLIENT -q "drop table t_missing_marks sync;"
