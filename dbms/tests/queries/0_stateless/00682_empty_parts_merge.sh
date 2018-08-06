#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.ordinary"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.ordinary(k UInt32) ENGINE MergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.ordinary(k) VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.ordinary(k) VALUES (1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.ordinary DELETE WHERE k = 1"
wait_for_mutation "ordinary" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE test.ordinary PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.ordinary"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.ordinary"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.summing"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.summing(k UInt32, v UInt32) ENGINE SummingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.summing(k, v) VALUES (1, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.summing(k, v) VALUES (1, 2)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.summing DELETE WHERE k = 1"
wait_for_mutation "summing" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE test.summing PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.summing"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.summing"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.aggregating"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.aggregating(k UInt32, v AggregateFunction(count)) ENGINE AggregatingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.aggregating(k) VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.aggregating(k) VALUES (1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.aggregating DELETE WHERE k = 1"
wait_for_mutation "aggregating" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE test.aggregating PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.aggregating"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.aggregating"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.replacing"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.replacing(k UInt32, v String) ENGINE ReplacingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.replacing(k, v) VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.replacing(k, v) VALUES (1, 'b')"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.replacing DELETE WHERE k = 1"
wait_for_mutation "replacing" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE test.replacing PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.replacing"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.replacing"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.collapsing"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.collapsing(k UInt32, v String, s Int8) ENGINE CollapsingMergeTree(s) ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.collapsing(k, v, s) VALUES (1, 'a', 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.collapsing(k, v, s) VALUES (2, 'b', 1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.collapsing DELETE WHERE k IN (1, 2)"
wait_for_mutation "collapsing" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE test.collapsing PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.collapsing"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.collapsing"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.versioned_collapsing"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.versioned_collapsing(k UInt32, val String, ver UInt32, s Int8) ENGINE VersionedCollapsingMergeTree(s, ver) ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.versioned_collapsing(k, val, ver, s) VALUES (1, 'a', 0, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.versioned_collapsing(k, val, ver, s) VALUES (2, 'b', 0, 1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE test.versioned_collapsing DELETE WHERE k IN (1, 2)"
wait_for_mutation "versioned_collapsing" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE test.versioned_collapsing PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.versioned_collapsing"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.versioned_collapsing"
