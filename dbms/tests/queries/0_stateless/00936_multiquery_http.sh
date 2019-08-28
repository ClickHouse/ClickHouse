#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo 'SELECT 1; SELECT 2; SELECT 3;' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}?multiquery=true" -d @-

echo '
DROP TABLE IF EXISTS test.test_table_1;
DROP TABLE IF EXISTS test.test_table_2;
DROP TABLE IF EXISTS test.test_table_3;
DROP TABLE IF EXISTS test.test_table_4;
DROP TABLE IF EXISTS test.test_table_5;
' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}?multiquery=true" -d @-


echo '
CREATE TABLE test.test_table_1 (x String) ENGINE = Memory;
CREATE TABLE test.test_table_2 (x String) ENGINE = Memory;
CREATE TABLE test.test_table_3 (x String) ENGINE = Memory;
CREATE TABLE test.test_table_4 (x String) ENGINE = Memory;
CREATE TABLE test.test_table_5 (x String) ENGINE = Memory;
' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}?multiquery=true" -d @-

COUNT=$(echo "
select count(*) from system.tables where name in (
  'test_table_1',
  'test_table_2',
  'test_table_3',
  'test_table_4',
  'test_table_5'
) and database = 'test';
" | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL} -d @-)
echo ${COUNT}
if [ ${COUNT} -ne 5 ];
then
  echo "MultiQuery failed"
  exit 1
fi

echo '
DROP TABLE test.test_table_1;
DROP TABLE test.test_table_2;
DROP TABLE test.test_table_3;
DROP TABLE test.test_table_4;
DROP TABLE test.test_table_5;
' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}?multiquery=true" -d @-

COUNT=$(echo "
select count(*) from system.tables where name in (
  'test_table_1',
  'test_table_2',
  'test_table_3',
  'test_table_4',
  'test_table_5'
) and database = 'test';
" | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL} -d @-)
echo ${COUNT}
if [ ${COUNT} -ne 0 ];
then
  echo "MultiQuery failed"
  exit 1
fi
