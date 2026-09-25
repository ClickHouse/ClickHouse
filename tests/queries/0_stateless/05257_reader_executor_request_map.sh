#!/usr/bin/env bash
# Tags: no-distributed-cache, no-encrypted-storage, no-parallel-replicas
# The executor falls back to the legacy read path for the distributed cache and for decryption, so nothing is announced.
# Parallel replicas learn their ranges from the coordinator only after the readers exist.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The distinct range counts of the request maps that the executor receives while it runs the query.
function range_counts()
{
    $CLICKHOUSE_CLIENT --send_logs_level=test --use_reader_executor=1 --max_threads=1 \
        --remote_filesystem_read_method=read --local_filesystem_read_method=pread --enable_filesystem_cache=0 \
        -q "$1" 2>&1 >/dev/null | grep -o 'Request map of .*, range count [0-9]*' | grep -o 'range count [0-9]*' | sort -u
}

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_wide (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
    CREATE TABLE t_compact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1G', min_bytes_for_full_part_storage = 0;
    CREATE TABLE t_packed (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = '1G';
"

for table in t_wide t_compact t_packed
do
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table SELECT number, number FROM numbers(100000)"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE $table FINAL"

    # In a packed part, the view of each file first announces its whole slice of the archive (range count 1).
    echo "$table: two key ranges"
    range_counts "SELECT sum(v) FROM $table WHERE k < 5000 OR k >= 90000"
    echo "$table: full scan"
    range_counts "SELECT sum(v) FROM $table"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_wide; DROP TABLE t_compact; DROP TABLE t_packed"
