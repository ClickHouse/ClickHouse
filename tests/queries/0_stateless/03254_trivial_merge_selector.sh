#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This is a smoke test, it proves that the Trivial merge selector exists and does something.

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_selector_algorithm = 'Trivial';
INSERT INTO test VALUES (1);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (2);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (3);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (4);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (5);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (6);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (7);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (8);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (9);
SELECT x FROM test ORDER BY x;
SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase();
INSERT INTO test VALUES (10);
SELECT x FROM test ORDER BY x;
OPTIMIZE TABLE test;
"

while true
do
    ${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE active AND table = 'test' AND database = currentDatabase() AND name = 'all_1_10_1'" | grep . && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "
DROP TABLE test;
"
