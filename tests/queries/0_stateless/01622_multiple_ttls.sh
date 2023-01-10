#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./merges.lib
. "$CURDIR"/merges.lib
set -e

$CLICKHOUSE_CLIENT --query "SELECT 'TTL WHERE';"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ttl_where;"

$CLICKHOUSE_CLIENT --multiquery --query '
CREATE TABLE ttl_where
(
    d Date,
    i UInt32
)
ENGINE = MergeTree
ORDER BY tuple()
TTL d + toIntervalYear(10) DELETE WHERE i % 3 = 0,
    d + toIntervalYear(40) DELETE WHERE i % 3 = 1;
'

# This test will fail at 2040-10-10

$CLICKHOUSE_CLIENT --multiquery --query "
INSERT INTO ttl_where SELECT toDate('2000-10-10'), number FROM numbers(10);
INSERT INTO ttl_where SELECT toDate('1970-10-10'), number FROM numbers(10);
"

$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE ttl_where FINAL;"
wait_for_merges_done ttl_where

$CLICKHOUSE_CLIENT --query "SELECT * FROM ttl_where ORDER BY d, i;"

$CLICKHOUSE_CLIENT --query "DROP TABLE ttl_where;"

$CLICKHOUSE_CLIENT --query "SELECT 'TTL GROUP BY';"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ttl_group_by;"

$CLICKHOUSE_CLIENT --query '
CREATE TABLE ttl_group_by
(
    d Date,
    i UInt32,
    v UInt64
)
ENGINE = MergeTree
ORDER BY (toStartOfMonth(d), i % 10)
TTL d + toIntervalYear(10) GROUP BY toStartOfMonth(d), i % 10 SET d = any(toStartOfMonth(d)), i = any(i % 10), v = sum(v),
    d + toIntervalYear(40) GROUP BY toStartOfMonth(d) SET d = any(toStartOfMonth(d)), v = sum(v);
'

$CLICKHOUSE_CLIENT --multiquery --query "
INSERT INTO ttl_group_by SELECT toDate('2000-10-10'), number, number FROM numbers(100);
INSERT INTO ttl_group_by SELECT toDate('1970-10-10'), number, number FROM numbers(100);
"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE ttl_group_by FINAL;"
wait_for_merges_done ttl_group_by

$CLICKHOUSE_CLIENT --query "SELECT * FROM ttl_group_by ORDER BY d, i;"

$CLICKHOUSE_CLIENT --query "DROP TABLE ttl_group_by;"
