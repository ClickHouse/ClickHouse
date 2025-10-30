#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS lowString;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS string;"

$CLICKHOUSE_CLIENT --query="
create table lowString
(
a LowCardinality(String),
b Date
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(b)
ORDER BY (a)"

$CLICKHOUSE_CLIENT --query="
create table string
(
a String,
b Date
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(b)
ORDER BY (a)"

$CLICKHOUSE_CLIENT --query="insert into lowString (a, b) select top 100000 toString(number), today() from system.numbers"

$CLICKHOUSE_CLIENT --query="insert into string (a, b) select top 100000 toString(number), today() from system.numbers"

$CLICKHOUSE_CLIENT --query="select count() from lowString where a in ('1', '2') SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0 FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="select count() from string where a in ('1', '2') SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0 FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="DROP TABLE lowString;"
$CLICKHOUSE_CLIENT --query="DROP TABLE string;"
