#!/usr/bin/env bash
# Tags: long, no-s3-storage-with-slow-build

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS bug;
    CREATE TABLE bug (UserID UInt64, Date Date) ENGINE = MergeTree ORDER BY Date
        SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', merge_max_block_size = 8192;
    INSERT INTO bug SELECT rand64(), '2020-06-07' FROM numbers(50000000);
    OPTIMIZE TABLE bug FINAL;"
LOG="$CLICKHOUSE_TMP/err-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_BENCHMARK --iterations 10 --max_threads 100 --min_bytes_to_use_direct_io 1 <<< "SELECT sum(UserID) FROM bug PREWHERE NOT ignore(Date)" 1>/dev/null 2>"$LOG"
cat "$LOG" | grep Exception
cat "$LOG" | grep Loaded

rm "$LOG"

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE bug;"
