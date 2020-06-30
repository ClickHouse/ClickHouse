#!/usr/bin/env bash


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh



$CLICKHOUSE_CLIENT --multiquery <<EOF
CREATE TABLE pk_in_tuple_perf
(
    v UInt64,
    u UInt32
) ENGINE = MergeTree()
ORDER BY v
SETTINGS index_granularity = 1;

INSERT INTO pk_in_tuple_perf SELECT number, number * 10 FROM numbers(100);
EOF

query="SELECT count() FROM pk_in_tuple_perf WHERE (v, u) IN ((2, 10), (2, 20))"

$CLICKHOUSE_CLIENT --query "$query"
$CLICKHOUSE_CLIENT --query "$query FORMAT JSON" | grep "rows_read"