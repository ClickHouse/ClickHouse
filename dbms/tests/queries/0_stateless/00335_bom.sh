#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo 'DROP TABLE IF EXISTS test.bom' | curl -sS 'http://localhost:8123/' --data-binary @-
echo 'CREATE TABLE test.bom (a UInt8, b UInt8, c UInt8) ENGINE = Memory' | curl -sS 'http://localhost:8123/' --data-binary @-
echo -ne '1,2,3\n' | curl -sS 'http://localhost:8123/?query=INSERT+INTO+test.bom+FORMAT+CSV' --data-binary @-
echo -ne '\xEF\xBB\xBF4,5,6\n' | curl -sS 'http://localhost:8123/?query=INSERT+INTO+test.bom+FORMAT+CSV' --data-binary @-
echo 'SELECT * FROM test.bom ORDER BY a' | curl -sS 'http://localhost:8123/' --data-binary @-
echo 'DROP TABLE test.bom' | curl -sS 'http://localhost:8123/' --data-binary @-
