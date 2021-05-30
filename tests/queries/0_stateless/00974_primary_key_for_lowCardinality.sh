#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS lowString;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS string;"

$CLICKHOUSE_CLIENT -n --query="
create table lowString
(
a LowCardinality(String),
b Date
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(b)
ORDER BY (a)"

$CLICKHOUSE_CLIENT -n --query="
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

$CLICKHOUSE_CLIENT --query="select count() from lowString where a in ('1', '2') FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="select count() from string where a in ('1', '2') FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="DROP TABLE lowString;"
$CLICKHOUSE_CLIENT --query="DROP TABLE string;"
