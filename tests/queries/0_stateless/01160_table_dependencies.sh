#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "drop table if exists dict_src;"
$CLICKHOUSE_CLIENT -q "drop dictionary if exists dict1;"
$CLICKHOUSE_CLIENT -q "drop dictionary if exists dict2;"
$CLICKHOUSE_CLIENT -q "drop table if exists join;"
$CLICKHOUSE_CLIENT -q "drop table if exists t;"

$CLICKHOUSE_CLIENT -q "create table dict_src (n int, m int, s String) engine=MergeTree order by n;"

$CLICKHOUSE_CLIENT -q "create dictionary dict1 (n int default 0, m int default 1, s String default 'qqq')
PRIMARY KEY n
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_src' PASSWORD '' DB '$CLICKHOUSE_DATABASE'))
LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());"

$CLICKHOUSE_CLIENT -q "create table join(n int, m int default dictGet('$CLICKHOUSE_DATABASE.dict1', 'm', 42::UInt64)) engine=Join(any, left, n);"

$CLICKHOUSE_CLIENT -q "create dictionary dict2 (n int default 0, m int DEFAULT 2, s String default 'asd')
PRIMARY KEY n
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'join' PASSWORD '' DB '$CLICKHOUSE_DATABASE'))
LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());"

$CLICKHOUSE_CLIENT -q "create table s (x default joinGet($CLICKHOUSE_DATABASE.join, 'm', 42::int)) engine=Set"

$CLICKHOUSE_CLIENT -q "create table t (n int, m int default joinGet($CLICKHOUSE_DATABASE.join, 'm', 42::int),
s String default dictGet($CLICKHOUSE_DATABASE.dict1, 's', 42::UInt64), x default in(1, $CLICKHOUSE_DATABASE.s)) engine=MergeTree order by n;"

$CLICKHOUSE_CLIENT -q "create materialized view mv to s as select n from t where n in (select n from join)"

CLICKHOUSE_CLIENT_DEFAULT_DB=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--database=${CLICKHOUSE_DATABASE}"'/--database=default/g')

for _ in {1..10}; do
  $CLICKHOUSE_CLIENT_DEFAULT_DB -q "detach database $CLICKHOUSE_DATABASE;"
  $CLICKHOUSE_CLIENT_DEFAULT_DB -q "attach database $CLICKHOUSE_DATABASE;"
done
$CLICKHOUSE_CLIENT -q "show tables from $CLICKHOUSE_DATABASE;"

$CLICKHOUSE_CLIENT -q "drop table dict_src;"
$CLICKHOUSE_CLIENT -q "drop dictionary dict1;"
$CLICKHOUSE_CLIENT -q "drop dictionary dict2;"
$CLICKHOUSE_CLIENT -q "drop table join;"
$CLICKHOUSE_CLIENT -q "drop table t;"
