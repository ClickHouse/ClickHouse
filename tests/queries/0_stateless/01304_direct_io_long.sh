#!/usr/bin/env bash
# Tags: long, no-object-storage-with-slow-build, no-flaky-check
# It can be too long with ThreadFuzzer

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --max_rows_to_read 50M --query "
    DROP TABLE IF EXISTS bug;
    CREATE TABLE bug (UserID UInt64, Date Date) ENGINE = MergeTree ORDER BY Date
        SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', merge_max_block_size = 8192;
    INSERT INTO bug SELECT rand64(), '2020-06-07' FROM numbers(50000000);
    OPTIMIZE TABLE bug FINAL;"
LOG="$CLICKHOUSE_TMP/err-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_BENCHMARK --max_rows_to_read 51M --iterations 10 --max_threads 100 --min_bytes_to_use_direct_io 1 <<< "SELECT sum(UserID) FROM bug PREWHERE NOT ignore(Date)" 1>/dev/null 2>"$LOG"
cat "$LOG" | grep Exception
cat "$LOG" | grep Loaded

rm "$LOG"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE bug;"
