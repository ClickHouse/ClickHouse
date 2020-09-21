#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --multiquery << EOF
DROP TABLE IF EXISTS mutations;
DROP TABLE IF EXISTS for_subquery;

CREATE TABLE mutations(x UInt32, y UInt32) ENGINE MergeTree ORDER BY x;
INSERT INTO mutations VALUES (123, 1), (234, 2), (345, 3);

CREATE TABLE for_subquery(x UInt32) ENGINE TinyLog;
INSERT INTO for_subquery VALUES (234), (345);

ALTER TABLE mutations UPDATE y = y + 1 WHERE x IN for_subquery;
ALTER TABLE mutations UPDATE y = y + 1 WHERE x IN (SELECT x FROM for_subquery);
EOF

wait_for_mutation "mutations" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM mutations"

${CLICKHOUSE_CLIENT} --query="DROP TABLE mutations"
${CLICKHOUSE_CLIENT} --query="DROP TABLE for_subquery"
