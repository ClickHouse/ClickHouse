#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} -sS 'http://localhost:8123/?query=DROP+TABLE' -d 'IF EXISTS test.insert'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/?query=CREATE' -d 'TABLE test.insert (x UInt8) ENGINE = Memory'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/' -d 'INSERT INTO test.insert VALUES (1),(2)'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/?query=INSERT+INTO+test.insert+VALUES' -d '(3),(4)'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/?query=INSERT+INTO+test.insert' -d 'VALUES (5),(6)'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/?query=INSERT+INTO+test.insert+VALUES+(7)' -d ',(8)'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/?query=INSERT+INTO+test.insert+VALUES+(9),(10)' -d ' '
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/' -d 'SELECT x FROM test.insert ORDER BY x'
${CLICKHOUSE_CURL} -sS 'http://localhost:8123/?query=DROP+TABLE' -d 'test.insert'
