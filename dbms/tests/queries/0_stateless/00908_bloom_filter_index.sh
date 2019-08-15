#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS bloom_filter_idx;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS bloom_filter_idx2;"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS bloom_filter_idx2;"


# NGRAM BF
$CLICKHOUSE_CLIENT -n --query="
SET allow_experimental_data_skipping_indices = 1;
CREATE TABLE bloom_filter_idx
(
    k UInt64,
    s String,
    INDEX bf (s, lower(s)) TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY k
SETTINGS index_granularity = 2;"

$CLICKHOUSE_CLIENT -n --query="
SET allow_experimental_data_skipping_indices = 1;
CREATE TABLE bloom_filter_idx2
(
    k UInt64,
    s FixedString(15),
    INDEX bf (s, lower(s)) TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY k
SETTINGS index_granularity = 2;"


$CLICKHOUSE_CLIENT --query="INSERT INTO bloom_filter_idx VALUES
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

$CLICKHOUSE_CLIENT --query="INSERT INTO bloom_filter_idx2 VALUES
(0, 'ClickHouse'),
(1, 'column-oriented'),
(2, 'column-oriented'),
(6, 'some string'),
(8, 'aбвгдеёж'),
(9, '2_2%2_2\\\\'),
(13, 'abc')"

# EQUAL
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx2 WHERE lower(s) = 'aбвгдеёж' OR s = 'aбвгдеёж' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx2 WHERE lower(s) = 'aбвгдеёж' OR s = 'aбвгдеёж' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s = 'aбвгдеёж' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s = 'aбвгдеёж' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE lower(s) = 'abc' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE lower(s) = 'abc' ORDER BY k FORMAT JSON" | grep "rows_read"

# LIKE
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%database%' AND s LIKE '%ClickHouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%database%' AND s LIKE '%ClickHouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%database%' AND lower(s) LIKE '%clickhouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%database%' AND lower(s) LIKE '%clickhouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%базами данных%' AND s LIKE '%ClickHouse%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%базами данных%' AND s LIKE '%ClickHouse%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE (s LIKE '%базами данных%' AND s LIKE '%ClickHouse%') OR s LIKE '____строка' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE (s LIKE '%базами данных%' AND s LIKE '%ClickHouse%') OR s LIKE '____строка' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%%<div>_%_%_</div>%%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%%<div>_%_%_</div>%%' ORDER BY k FORMAT JSON" | grep "rows_read"


$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%2\\\\%2%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%2\\\\%2%' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%_\\\\%2\\\\__\\\\' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '%_\\\\%2\\\\__\\\\' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '2\\\\_2\\\\%2_2\\\\' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '2\\\\_2\\\\%2_2\\\\' ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '2\\\\_2\\\\%2_2_' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s LIKE '2\\\\_2\\\\%2_2_' ORDER BY k FORMAT JSON" | grep "rows_read"

# IN
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s IN ('aбвгдеёж', 'abc') ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE s IN ('aбвгдеёж', 'abc') ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE (s, lower(s)) IN (('aбвгдеёж', 'aбвгдеёж'), ('abc', 'cba')) ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx WHERE (s, lower(s)) IN (('aбвгдеёж', 'aбвгдеёж'), ('abc', 'cba')) ORDER BY k FORMAT JSON" | grep "rows_read"


# TOKEN BF
$CLICKHOUSE_CLIENT -n --query="
SET allow_experimental_data_skipping_indices = 1;
CREATE TABLE bloom_filter_idx3
(
    k UInt64,
    s String,
    INDEX bf (s, lower(s)) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY k
SETTINGS index_granularity = 2;"

$CLICKHOUSE_CLIENT --query="INSERT INTO bloom_filter_idx3 VALUES
(0, 'ClickHouse is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).'),
(1, 'column-oriented'),
(2, 'column-oriented'),
(6, 'some string'),
(8, 'column with ints'),
(9, '2_2%2_2\\\\'),
(13, 'abc')"

# EQUAL
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx3 WHERE lower(s) = 'column-oriented' OR s = 'column-oriented' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx3 WHERE lower(s) = 'column-oriented' OR s = 'column-oriented' ORDER BY k FORMAT JSON" | grep "rows_read"

# LIKE
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx3 WHERE lower(s) LIKE '%(dbms)%' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx3 WHERE lower(s) LIKE '%(dbms)%' ORDER BY k FORMAT JSON" | grep "rows_read"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx3 WHERE s LIKE 'column-%' AND s LIKE '%-oriented' ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx3 WHERE s LIKE 'column-%' AND s LIKE '%-oriented' ORDER BY k FORMAT JSON" | grep "rows_read"

# IN
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx3 WHERE s IN ('some string', 'abc') ORDER BY k"
$CLICKHOUSE_CLIENT --query="SELECT * FROM bloom_filter_idx3 WHERE s IN ('some string', 'abc') ORDER BY k FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="DROP TABLE bloom_filter_idx"
$CLICKHOUSE_CLIENT --query="DROP TABLE bloom_filter_idx2"
$CLICKHOUSE_CLIENT --query="DROP TABLE bloom_filter_idx3"