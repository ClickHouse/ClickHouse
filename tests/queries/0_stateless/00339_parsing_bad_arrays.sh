#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'DROP TABLE IF EXISTS bad_arrays'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'CREATE TABLE bad_arrays (a Array(String)) ENGINE = Memory'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "INSERT INTO bad_arrays VALUES ([123]), (['123', concat('Hello', ' world!'), toString(123)])"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'SELECT * FROM bad_arrays ORDER BY a'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'DROP TABLE bad_arrays'
