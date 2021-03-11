#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

. "$CURDIR"/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} --multiquery --query="
DROP TABLE IF EXISTS view_00699;
DROP TABLE IF EXISTS null_00699;

CREATE TABLE null_00699 (x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW view_00699 ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM null_00699;

INSERT INTO null_00699 SELECT * FROM numbers(100);
SELECT count(), min(x), max(x) FROM null_00699;
SELECT count(), min(x), max(x) FROM view_00699;

ALTER TABLE null_00699 DELETE WHERE x % 2 = 0;"

wait_for_mutation null_00699 mutation_2.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null_00699;
SELECT count(), min(x), max(x) FROM view_00699;

ALTER TABLE view_00699 DELETE WHERE x % 2 = 0;
"

wait_for_mutation .inner.view_00699 mutation_2.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null_00699;
SELECT count(), min(x), max(x) FROM view_00699;

ALTER TABLE null_00699 DELETE WHERE x % 2 = 1;
ALTER TABLE view_00699 DELETE WHERE x % 2 = 1;
"

wait_for_mutation null_00699 mutation_3.txt
wait_for_mutation .inner.view_00699 mutation_3.txt

${CLICKHOUSE_CLIENT} --multiquery --query="
SELECT count(), min(x), max(x) FROM null_00699;
SELECT count(), min(x), max(x) FROM view_00699;

DROP TABLE view_00699;
DROP TABLE null_00699;
"
