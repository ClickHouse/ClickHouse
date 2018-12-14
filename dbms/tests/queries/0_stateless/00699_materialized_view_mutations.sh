#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} --multiquery --query="
DROP TABLE IF EXISTS test.view;
DROP TABLE IF EXISTS test.null;

CREATE TABLE test.null (x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW test.view ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM test.null;

INSERT INTO test.null SELECT * FROM numbers(100);
SELECT count(), min(x), max(x) FROM test.null;
SELECT count(), min(x), max(x) FROM test.view;

ALTER TABLE test.null DELETE WHERE x % 2 = 0;"

wait_for_mutation null mutation_2.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM test.null;
SELECT count(), min(x), max(x) FROM test.view;

ALTER TABLE test.view DELETE WHERE x % 2 = 0;
"

wait_for_mutation .inner.view mutation_2.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM test.null;
SELECT count(), min(x), max(x) FROM test.view;

ALTER TABLE test.null DELETE WHERE x % 2 = 1;
ALTER TABLE test.view DELETE WHERE x % 2 = 1;
"

wait_for_mutation null mutation_3.txt
wait_for_mutation .inner.view mutation_3.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM test.null;
SELECT count(), min(x), max(x) FROM test.view;

DROP TABLE test.view;
DROP TABLE test.null;
"
