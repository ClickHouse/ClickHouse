#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS set_idx;"

$CLICKHOUSE_CLIENT -n --query="
CREATE TABLE set_idx
(
    k UInt64,
    s String,
    INDEX idx (s) TYPE set(2) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY k
SETTINGS index_granularity = 2;"

$CLICKHOUSE_CLIENT --query="INSERT INTO set_idx VALUES
(0, 'ClickHouse - столбцовая система управления базами данных (СУБД)'),
(1, 'ClickHouse is a column-oriented database management system (DBMS)'),
(2, 'column-oriented database management system'),
(3, 'columns'),
(4, 'какая-то строка'),
(5, 'еще строка'),
(6, 'some string'),
(7, 'another string'),
(8, 'computer science'),
(9, 'abra'),
(10, 'cadabra'),
(11, 'crabacadabra'),
(12, 'crab'),
(13, 'basement'),
(14, 'abracadabra'),
(15, 'cadabraabra')"

# STARTS_WITH
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE startsWith(s, 'abra')"
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE startsWith(s, 'abra') FORMAT JSON" | grep "rows_read"

# ENDS_WITH
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE endsWith(s, 'abra')"
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE endsWith(s, 'abra') FORMAT JSON" | grep "rows_read"

# COMBINED
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE startsWith(s, 'abra') AND endsWith(s, 'abra')"
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE startsWith(s, 'abra') AND endsWith(s, 'abra') FORMAT JSON" | grep "rows_read"

# MULTY_SEARCH_ANY
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE multiSearchAny(s, ['data', 'base'])"
$CLICKHOUSE_CLIENT --query="SELECT * FROM set_idx WHERE multiSearchAny(s, ['data', 'base']) FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="DROP TABLE set_idx;"
