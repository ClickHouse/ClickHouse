#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT -n --ignore-error --query="
DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;
DROP TABLE IF EXISTS test3;

CREATE TABLE test1 ( id String ) ENGINE = StripeLog;
CREATE TABLE test2 ( id String ) ENGINE = StripeLog;
INSERT INTO test2 VALUES ('a');
CREATE TABLE test3 ( id String, name String ) ENGINE = StripeLog;
INSERT INTO test3 VALUES ('a', 'aaa');

INSERT INTO test1 SELECT id, name FROM test2 ANY INNER JOIN test3 USING (id);
INSERT INTO test1 SELECT id, name FROM test2 ANY LEFT OUTER JOIN test3 USING (id);

DROP TABLE test1;
DROP TABLE test2;
DROP TABLE test3;
" --server_logs_file=/dev/null 2>&1 | grep -F "Number of columns doesn't match" | wc -l

$CLICKHOUSE_CLIENT --query="SELECT 1";
