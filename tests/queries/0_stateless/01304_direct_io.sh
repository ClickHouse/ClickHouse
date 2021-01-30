#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS bug;
    CREATE TABLE bug (UserID UInt64, Date Date) ENGINE = MergeTree ORDER BY Date;
    INSERT INTO bug SELECT rand64(), '2020-06-07' FROM numbers(50000000);
    OPTIMIZE TABLE bug FINAL;"

$CLICKHOUSE_BENCHMARK --iterations 10 --max_threads 100 --min_bytes_to_use_direct_io 1 <<< "SELECT sum(UserID) FROM bug PREWHERE NOT ignore(Date)" 1>/dev/null 2>"$CLICKHOUSE_TMP"/err
cat "$CLICKHOUSE_TMP"/err | grep Exception
cat "$CLICKHOUSE_TMP"/err | grep Loaded

rm "$CLICKHOUSE_TMP"/err

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE bug;"
