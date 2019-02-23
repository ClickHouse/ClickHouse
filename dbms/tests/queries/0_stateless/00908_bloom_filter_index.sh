#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.bloom_filter_idx;"


$CLICKHOUSE_CLIENT -n --query="
SET allow_experimental_data_skipping_indices = 1;
CREATE TABLE test.bloom_filter_idx
(
    k UInt64,
    s String,
    INDEX bf (s, lower(s)) TYPE ngrambf(3, 512, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY k
SETTINGS index_granularity = 2;"


$CLICKHOUSE_CLIENT --query="INSERT INTO test.bloom_filter_idx VALUES
(0, 'ClickHouse - столбцовая система управления базами данных (СУБД) для онлайн обработки аналитических запросов (OLAP).'),
(1, 'ClickHouse is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).'),
(2, 'column-oriented database management system'),
(3, 'столбцовая система управления базами данных'),
(4, 'какая-то строка'),
(5, 'еще строка'),
(6, 'some string'),
(7, 'another string'),
(8, 'aбвгдеёж'),
(9, '2_2%2_2\\\\'),
(11, '!@#$%^&*0123456789'),
(12, '<div> странный <strong>html</strong> </div>'),
(13, 'abc')"


$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s = 'aбвгдеёж' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s = 'aбвгдеёж' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s = 'abc' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s = 'abc' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%database%' AND s LIKE '%ClickHouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%database%' AND s LIKE '%ClickHouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%database%' AND lower(s) LIKE '%clickhouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%database%' AND lower(s) LIKE '%clickhouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%базами данных%' AND s LIKE '%ClickHouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%базами данных%' AND s LIKE '%ClickHouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE (s LIKE '%базами данных%' AND s LIKE '%ClickHouse%') OR s LIKE '____строка' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE (s LIKE '%базами данных%' AND s LIKE '%ClickHouse%') OR s LIKE '____строка' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%%<div>_%_%_</div>%%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%%<div>_%_%_</div>%%' ORDER BY k FORMAT JSON" | grep "rows_read"


$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%2\\\\%2%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%2\\\\%2%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%_\\\\%2\\\\__\\\\' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '%_\\\\%2\\\\__\\\\' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '2\\\\_2\\\\%2_2\\\\' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '2\\\\_2\\\\%2_2\\\\' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '2\\\\_2\\\\%2_2_' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.bloom_filter_idx WHERE s LIKE '2\\\\_2\\\\%2_2_' ORDER BY k FORMAT JSON" | grep "rows_read"


$CLICKHOUSE_CLIENT --query="DROP TABLE test.bloom_filter_idx"