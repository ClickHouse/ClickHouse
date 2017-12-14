#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} -sS 'http://localhost:8123/' -d 'DROP TABLE IF EXISTS test.bad_arrays'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/' -d 'CREATE TABLE test.bad_arrays (a Array(String)) ENGINE = Memory'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/' -d 'INSERT INTO test.bad_arrays VALUES ([123])' 2>&1 | grep -c 'Exception'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/' -d "INSERT INTO test.bad_arrays VALUES (['123', concat('Hello', ' world!'), toString(123)])"
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/' -d 'SELECT * FROM test.bad_arrays'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/' -d 'DROP TABLE test.bad_arrays'
