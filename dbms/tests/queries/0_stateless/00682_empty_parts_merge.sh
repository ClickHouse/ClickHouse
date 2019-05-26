#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ordinary"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ordinary(k UInt32) ENGINE MergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO ordinary(k) VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO ordinary(k) VALUES (1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE ordinary DELETE WHERE k = 1"
wait_for_mutation "ordinary" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE ordinary PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM ordinary"

${CLICKHOUSE_CLIENT} --query="DROP TABLE ordinary"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Vertical merge ***'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS vertical"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE vertical(k UInt32, v UInt32) ENGINE MergeTree ORDER BY k \
    SETTINGS enable_vertical_merge_algorithm=1, \
             vertical_merge_algorithm_min_rows_to_activate=0, \
             vertical_merge_algorithm_min_columns_to_activate=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO vertical(k, v) VALUES (1, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO vertical(k, v) VALUES (2, 2)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE vertical DELETE WHERE k = 1"
wait_for_mutation "vertical" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE vertical PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM vertical"

${CLICKHOUSE_CLIENT} --query="DROP TABLE vertical"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS summing"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE summing(k UInt32, v UInt32) ENGINE SummingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO summing(k, v) VALUES (1, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO summing(k, v) VALUES (1, 2)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE summing DELETE WHERE k = 1"
wait_for_mutation "summing" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE summing PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM summing"

${CLICKHOUSE_CLIENT} --query="DROP TABLE summing"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS aggregating"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE aggregating(k UInt32, v AggregateFunction(count)) ENGINE AggregatingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO aggregating(k) VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO aggregating(k) VALUES (1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE aggregating DELETE WHERE k = 1"
wait_for_mutation "aggregating" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE aggregating PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM aggregating"

${CLICKHOUSE_CLIENT} --query="DROP TABLE aggregating"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS replacing"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE replacing(k UInt32, v String) ENGINE ReplacingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO replacing(k, v) VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO replacing(k, v) VALUES (1, 'b')"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE replacing DELETE WHERE k = 1"
wait_for_mutation "replacing" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE replacing PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM replacing"

${CLICKHOUSE_CLIENT} --query="DROP TABLE replacing"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS collapsing"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE collapsing(k UInt32, v String, s Int8) ENGINE CollapsingMergeTree(s) ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO collapsing(k, v, s) VALUES (1, 'a', 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO collapsing(k, v, s) VALUES (2, 'b', 1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE collapsing DELETE WHERE k IN (1, 2)"
wait_for_mutation "collapsing" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE collapsing PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM collapsing"

${CLICKHOUSE_CLIENT} --query="DROP TABLE collapsing"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS versioned_collapsing"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE versioned_collapsing(k UInt32, val String, ver UInt32, s Int8) ENGINE VersionedCollapsingMergeTree(s, ver) ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO versioned_collapsing(k, val, ver, s) VALUES (1, 'a', 0, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO versioned_collapsing(k, val, ver, s) VALUES (2, 'b', 0, 1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE versioned_collapsing DELETE WHERE k IN (1, 2)"
wait_for_mutation "versioned_collapsing" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE versioned_collapsing PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM versioned_collapsing"

${CLICKHOUSE_CLIENT} --query="DROP TABLE versioned_collapsing"
