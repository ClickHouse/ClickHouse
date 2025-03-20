#!/usr/bin/env bash
# Tags: long, no-random-settings, no-distributed-cache

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_index_hypothesis"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_index_hypothesis (a UInt32, b UInt32, INDEX t a != b TYPE hypothesis GRANULARITY 1) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_index_hypothesis SELECT number, number + 1 FROM numbers(10000000)"

run_query() {
    output=`$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_index_hypothesis WHERE a = b"`
    if [[ $output != "0" ]]; then
        echo "output: $output, expected: 0"
        exit 1
    fi
}

export -f run_query
parallel -j 8 run_query ::: {0..30}

if [ $? -ne 0 ]; then
    echo FAILED
    exit 1
fi

echo OK

$CLICKHOUSE_CLIENT -q "DROP TABLE t_index_hypothesis"
