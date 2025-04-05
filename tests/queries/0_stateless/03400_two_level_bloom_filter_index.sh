#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: grep -c

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS two_level_bloom_filter_idx;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS two_level_bloom_filter_idx2;"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE two_level_bloom_filter_idx
(
    k UInt64,
    s String,
    INDEX bf (s, lower(s)) TYPE ngrambf_v1(3, 512, 2, 0, 3) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY k
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';"

$CLICKHOUSE_CLIENT --query="
CREATE TABLE two_level_bloom_filter_idx2
(
    k UInt64,
    s String,
    INDEX bf (s, lower(s)) TYPE ngrambf_v1(3, 512, 2, 0, 3, 512, 2, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY k
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';"

$CLICKHOUSE_CLIENT --query="INSERT INTO two_level_bloom_filter_idx VALUES
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

$CLICKHOUSE_CLIENT --query="INSERT INTO two_level_bloom_filter_idx2 VALUES
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


$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE lower(s) = 'aбвгдеёж' OR s = 'aбвгдеёж' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE lower(s) = 'aбвгдеёж' OR s = 'aбвгдеёж' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE s = 'aбвгдеёж' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE s = 'aбвгдеёж' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE lower(s) = 'abc' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE lower(s) = 'abc' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE s LIKE '%database%' AND s LIKE '%ClickHouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE s LIKE '%database%' AND s LIKE '%ClickHouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE s LIKE '%database%' AND lower(s) LIKE '%clickhouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE s LIKE '%database%' AND lower(s) LIKE '%clickhouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE s LIKE '%базами данных%' AND s LIKE '%ClickHouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE s LIKE '%базами данных%' AND s LIKE '%ClickHouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE (s LIKE '%базами данных%' AND s LIKE '%ClickHouse%') OR s LIKE '____строка' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx WHERE (s LIKE '%базами данных%' AND s LIKE '%ClickHouse%') OR s LIKE '____строка' ORDER BY k FORMAT JSON" | grep "rows_read"

# Second table

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE lower(s) = 'aбвгдеёж' OR s = 'aбвгдеёж' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE lower(s) = 'aбвгдеёж' OR s = 'aбвгдеёж' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE s = 'aбвгдеёж' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE s = 'aбвгдеёж' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE lower(s) = 'abc' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE lower(s) = 'abc' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE s LIKE '%database%' AND s LIKE '%ClickHouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE s LIKE '%database%' AND s LIKE '%ClickHouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE s LIKE '%database%' AND lower(s) LIKE '%clickhouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE s LIKE '%database%' AND lower(s) LIKE '%clickhouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE s LIKE '%базами данных%' AND s LIKE '%ClickHouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE s LIKE '%базами данных%' AND s LIKE '%ClickHouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE (s LIKE '%базами данных%' AND s LIKE '%ClickHouse%') OR s LIKE '____строка' ORDER BY k"
$CLICKHOUSE_CLIENT --optimize_or_like_chain 0 --query="SELECT * FROM two_level_bloom_filter_idx2 WHERE (s LIKE '%базами данных%' AND s LIKE '%ClickHouse%') OR s LIKE '____строка' ORDER BY k FORMAT JSON" | grep "rows_read"
