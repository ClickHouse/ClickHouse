#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'DROP TABLE IF EXISTS test.bad_arrays'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'CREATE TABLE test.bad_arrays (a Array(String)) ENGINE = Memory'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'INSERT INTO test.bad_arrays VALUES ([123])' 2>&1 | grep -c 'Exception'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "INSERT INTO test.bad_arrays VALUES (['123', concat('Hello', ' world!'), toString(123)])"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT * FROM test.bad_arrays'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'DROP TABLE test.bad_arrays'
