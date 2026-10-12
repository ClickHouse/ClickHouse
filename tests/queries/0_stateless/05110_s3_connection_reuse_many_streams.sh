#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-fasttest: needs an S3 disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every stream of a Wide part is read up to the end of its data file. The read buffer must pass the exact right bound
# to the object storage request even when it coincides with the end of the object, otherwise each stream holds its
# HTTP connection until the end of the query instead of returning it to the pool.

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS t_many_streams_s3;

CREATE TABLE t_many_streams_s3 (key UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64, c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64, c16 UInt64, c17 UInt64, c18 UInt64, c19 UInt64, c20 UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_many_streams_s3;

INSERT INTO t_many_streams_s3 SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1000);
"

# One thread with synchronous reads. Count the requests that are sent with the end of their range.
logs=$($CLICKHOUSE_CLIENT --send_logs_level=test -q "
SELECT * FROM t_many_streams_s3 FORMAT Null
SETTINGS max_threads = 1, remote_filesystem_read_method = 'read', load_marks_asynchronously = 0, use_page_cache_for_disks_without_file_cache = 0, enable_parallel_replicas = 0
" 2>&1 >/dev/null) || { echo "SELECT failed:"; grep -F 'Received exception' -A1 <<< "$logs"; }
grep -cE 'Read S3 object\..*, Range: [0-9]+-[0-9]+' <<< "$logs" | awk '{ print ($1 >= 20) }'

$CLICKHOUSE_CLIENT -q "DROP TABLE t_many_streams_s3"
