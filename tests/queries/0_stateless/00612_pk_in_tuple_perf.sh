#!/usr/bin/env bash


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh



$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS pk_in_tuple_perf;
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

## Test with non-const args in tuple

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS pk_in_tuple_perf_non_const;
CREATE TABLE pk_in_tuple_perf_non_const
(
    d Date,
    u UInt32
) ENGINE = MergeTree()
ORDER BY (u, d)
SETTINGS index_granularity = 1;

INSERT INTO pk_in_tuple_perf_non_const SELECT today() - number, number FROM numbers(100);
EOF

query="SELECT count() FROM pk_in_tuple_perf_non_const WHERE (u, d) IN ((0, today()), (1, today()))"

$CLICKHOUSE_CLIENT --query "$query"
$CLICKHOUSE_CLIENT --query "$query FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query "DROP TABLE pk_in_tuple_perf"
$CLICKHOUSE_CLIENT --query "DROP TABLE pk_in_tuple_perf_non_const"
