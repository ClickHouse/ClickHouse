#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} --multiquery --query="
DROP TABLE IF EXISTS view;
DROP TABLE IF EXISTS null;

CREATE TABLE null (x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW view ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM null;

INSERT INTO null SELECT * FROM numbers(100);
SELECT count(), min(x), max(x) FROM null;
SELECT count(), min(x), max(x) FROM view;

ALTER TABLE null DELETE WHERE x % 2 = 0;"

wait_for_mutation null mutation_2.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null;
SELECT count(), min(x), max(x) FROM view;

ALTER TABLE view DELETE WHERE x % 2 = 0;
"

wait_for_mutation .inner.view mutation_2.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null;
SELECT count(), min(x), max(x) FROM view;

ALTER TABLE null DELETE WHERE x % 2 = 1;
ALTER TABLE view DELETE WHERE x % 2 = 1;
"

wait_for_mutation null mutation_3.txt
wait_for_mutation .inner.view mutation_3.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null;
SELECT count(), min(x), max(x) FROM view;

DROP TABLE view;
DROP TABLE null;
"
