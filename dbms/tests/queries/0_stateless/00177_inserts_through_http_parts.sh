#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

curl -sS 'http://localhost:8123/?query=DROP+TABLE' -d 'IF EXISTS test.insert'
curl -sS 'http://localhost:8123/?query=CREATE' -d 'TABLE test.insert (x UInt8) ENGINE = Memory'
curl -sS 'http://localhost:8123/' -d 'INSERT INTO test.insert VALUES (1),(2)'
curl -sS 'http://localhost:8123/?query=INSERT+INTO+test.insert+VALUES' -d '(3),(4)'
curl -sS 'http://localhost:8123/?query=INSERT+INTO+test.insert' -d 'VALUES (5),(6)'
curl -sS 'http://localhost:8123/?query=INSERT+INTO+test.insert+VALUES+(7)' -d ',(8)'
curl -sS 'http://localhost:8123/?query=INSERT+INTO+test.insert+VALUES+(9),(10)' -d ' '
curl -sS 'http://localhost:8123/' -d 'SELECT x FROM test.insert ORDER BY x'
curl -sS 'http://localhost:8123/?query=DROP+TABLE' -d 'test.insert'
