#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n --ignore-error --query="
DROP TABLE IF EXISTS test1_00550;
DROP TABLE IF EXISTS test2_00550;
DROP TABLE IF EXISTS test3_00550;

CREATE TABLE test1_00550 ( id String ) ENGINE = StripeLog;
CREATE TABLE test2_00550 ( id String ) ENGINE = StripeLog;
INSERT INTO test2_00550 VALUES ('a');
CREATE TABLE test3_00550 ( id String, name String ) ENGINE = StripeLog;
INSERT INTO test3_00550 VALUES ('a', 'aaa');

INSERT INTO test1_00550 SELECT id, name FROM test2_00550 ANY INNER JOIN test3_00550 USING (id) SETTINGS any_join_distinct_right_table_keys=1;
INSERT INTO test1_00550 SELECT id, name FROM test2_00550 ANY LEFT OUTER JOIN test3_00550 USING (id);

DROP TABLE test1_00550;
DROP TABLE test2_00550;
DROP TABLE test3_00550;
" --server_logs_file=/dev/null 2>&1 | grep -F "Number of columns doesn't match" | wc -l

$CLICKHOUSE_CLIENT --query="SELECT 1";
