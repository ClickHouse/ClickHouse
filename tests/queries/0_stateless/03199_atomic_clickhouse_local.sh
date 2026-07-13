#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -n "
CREATE TABLE test (x UInt8) ORDER BY x;
INSERT INTO test VALUES (123);
SELECT * FROM test;
CREATE OR REPLACE TABLE test (s String) ORDER BY s;
INSERT INTO test VALUES ('Hello');
SELECT * FROM test;
RENAME TABLE test TO test2;
CREATE OR REPLACE TABLE test (s Array(String)) ORDER BY s;
INSERT INTO test VALUES (['Hello', 'world']);
SELECT * FROM test;
SELECT * FROM test2;
EXCHANGE TABLES test AND test2;
SELECT * FROM test;
SELECT * FROM test2;
DROP TABLE test;
DROP TABLE test2;
"
