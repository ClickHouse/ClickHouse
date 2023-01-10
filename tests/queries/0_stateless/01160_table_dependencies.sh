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

$CLICKHOUSE_CLIENT -q "create dictionary dict2 (n int default 0, m int DEFAULT 2)
PRIMARY KEY n
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'join' PASSWORD '' DB '$CLICKHOUSE_DATABASE'))
LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());"

$CLICKHOUSE_CLIENT -q "create table s (x default joinGet($CLICKHOUSE_DATABASE.join, 'm', 42::int)) engine=Set"

$CLICKHOUSE_CLIENT -q "create table t (n int, m int default joinGet($CLICKHOUSE_DATABASE.join, 'm', 42::int),
s String default dictGet($CLICKHOUSE_DATABASE.dict1, 's', 42::UInt64), y default dictGet($CLICKHOUSE_DATABASE.dict2, 'm', 42::UInt64)) engine=MergeTree order by n;"

$CLICKHOUSE_CLIENT -q "select table, arraySort(dependencies_table),
arraySort(loading_dependencies_table), arraySort(loading_dependent_table) from system.tables where database=currentDatabase() order by table"
$CLICKHOUSE_CLIENT -q "select '====='"
$CLICKHOUSE_CLIENT -q "alter table t add column x int default in(1, $CLICKHOUSE_DATABASE.s), drop column y"

$CLICKHOUSE_CLIENT -q "create materialized view mv to s as select n from t where n in (select n from join)"

$CLICKHOUSE_CLIENT -q "select table, arraySort(dependencies_table),
arraySort(loading_dependencies_table), arraySort(loading_dependent_table) from system.tables where database=currentDatabase() order by table"

CLICKHOUSE_CLIENT_DEFAULT_DB=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--database=${CLICKHOUSE_DATABASE}"'/--database=default/g')

for _ in {1..10}; do
  $CLICKHOUSE_CLIENT_DEFAULT_DB -q "detach database $CLICKHOUSE_DATABASE;"
  $CLICKHOUSE_CLIENT_DEFAULT_DB -q "attach database $CLICKHOUSE_DATABASE;"
done
$CLICKHOUSE_CLIENT -q "show tables from $CLICKHOUSE_DATABASE;"

$CLICKHOUSE_CLIENT -q "rename table join to join1" 2>&1| grep -Fa "some tables depend on it" >/dev/null && echo "OK"

$CLICKHOUSE_CLIENT -q "drop table join" 2>&1| grep -Fa "some tables depend on it" >/dev/null && echo "OK"
$CLICKHOUSE_CLIENT -q "detach dictionary dict1 permanently" 2>&1| grep -Fa "some tables depend on it" >/dev/null && echo "OK"

$CLICKHOUSE_CLIENT -q "select table, arraySort(dependencies_table),
arraySort(loading_dependencies_table), arraySort(loading_dependent_table) from system.tables where database=currentDatabase() order by table"

engine=`$CLICKHOUSE_CLIENT -q "select engine from system.databases where name='${CLICKHOUSE_DATABASE}'"`
$CLICKHOUSE_CLIENT -q "drop database if exists ${CLICKHOUSE_DATABASE}_1"
if [[ $engine == "Atomic" ]]; then
    $CLICKHOUSE_CLIENT -q "rename database ${CLICKHOUSE_DATABASE} to ${CLICKHOUSE_DATABASE}_1" 2>&1| grep -Fa "some tables depend on it" >/dev/null && echo "OK"
else
    echo "OK"
fi

$CLICKHOUSE_CLIENT -q "drop table mv"
$CLICKHOUSE_CLIENT -q "create database ${CLICKHOUSE_DATABASE}_1"

t_database=${CLICKHOUSE_DATABASE}

if [[ $engine == "Atomic" ]]; then
    $CLICKHOUSE_CLIENT -q "rename table t to ${CLICKHOUSE_DATABASE}_1.t"
    $CLICKHOUSE_CLIENT -q "rename database ${CLICKHOUSE_DATABASE}_1 to ${CLICKHOUSE_DATABASE}_1_renamed"
    t_database="${CLICKHOUSE_DATABASE}_1_renamed"
fi

$CLICKHOUSE_CLIENT -q "select table, arraySort(dependencies_table),
arraySort(loading_dependencies_table), arraySort(loading_dependent_table) from system.tables where database in (currentDatabase(), '$t_database') order by table"

$CLICKHOUSE_CLIENT -q "drop table ${t_database}.t;"
$CLICKHOUSE_CLIENT -q "drop table s;"
$CLICKHOUSE_CLIENT -q "drop dictionary dict2;"

$CLICKHOUSE_CLIENT -q "select '====='"
$CLICKHOUSE_CLIENT -q "select table, arraySort(dependencies_table),
arraySort(loading_dependencies_table), arraySort(loading_dependent_table) from system.tables where database=currentDatabase() order by table"
if [[ $engine != "Ordinary" ]]; then
    $CLICKHOUSE_CLIENT -q "create or replace table dict_src (n int, m int, s String) engine=MergeTree order by (n, m);"
fi

$CLICKHOUSE_CLIENT -q "drop table join;"
$CLICKHOUSE_CLIENT -q "drop dictionary dict1;"
$CLICKHOUSE_CLIENT -q "drop table dict_src;"
if [[ $t_database != "$CLICKHOUSE_DATABASE" ]]; then
    $CLICKHOUSE_CLIENT -q "drop database if exists ${t_database}"
fi

$CLICKHOUSE_CLIENT -q "drop database if exists ${CLICKHOUSE_DATABASE}_1"
$CLICKHOUSE_CLIENT -q "create database ${CLICKHOUSE_DATABASE}_1"
$CLICKHOUSE_CLIENT -q "create table ${CLICKHOUSE_DATABASE}_1.xdict_src (n int, m int, s String) engine=MergeTree order by n;"
$CLICKHOUSE_CLIENT -q "create dictionary ${CLICKHOUSE_DATABASE}_1.ydict1 (n int default 0, m int default 1, s String default 'qqq')
PRIMARY KEY n
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'xdict_src' PASSWORD '' DB '${CLICKHOUSE_DATABASE}_1'))
LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());"

$CLICKHOUSE_CLIENT -q "create table ${CLICKHOUSE_DATABASE}_1.zjoin(n int, m int default dictGet('${CLICKHOUSE_DATABASE}_1.ydict1', 'm', 42::UInt64)) engine=Join(any, left, n);"
$CLICKHOUSE_CLIENT -q "drop database ${CLICKHOUSE_DATABASE}_1"
