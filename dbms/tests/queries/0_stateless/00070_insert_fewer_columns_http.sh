#!/usr/bin/env bash -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo 'DROP TABLE IF EXISTS test.insert_fewer_columns'                            | curl -sSg 'http://localhost:8123' -d @-
echo 'CREATE TABLE test.insert_fewer_columns (a UInt8, b UInt8) ENGINE = Memory' | curl -sSg 'http://localhost:8123' -d @-
echo 'INSERT INTO test.insert_fewer_columns (a) VALUES (1), (2)'                 | curl -sSg 'http://localhost:8123' -d @-
echo 'SELECT * FROM test.insert_fewer_columns'                                   | curl -sSg 'http://localhost:8123' -d @-
