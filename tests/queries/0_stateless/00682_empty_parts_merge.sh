#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

. "$CURDIR"/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS ordinary_00682"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ordinary_00682(k UInt32) ENGINE MergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO ordinary_00682(k) VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO ordinary_00682(k) VALUES (1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE ordinary_00682 DELETE WHERE k = 1"
wait_for_mutation "ordinary_00682" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE ordinary_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM ordinary_00682"

${CLICKHOUSE_CLIENT} --query="DROP TABLE ordinary_00682"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Vertical merge ***'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS vertical_00682"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE vertical_00682(k UInt32, v UInt32) ENGINE MergeTree ORDER BY k \
    SETTINGS enable_vertical_merge_algorithm=1, \
             vertical_merge_algorithm_min_rows_to_activate=0, \
             vertical_merge_algorithm_min_columns_to_activate=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO vertical_00682(k, v) VALUES (1, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO vertical_00682(k, v) VALUES (2, 2)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE vertical_00682 DELETE WHERE k = 1"
wait_for_mutation "vertical_00682" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE vertical_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM vertical_00682"

${CLICKHOUSE_CLIENT} --query="DROP TABLE vertical_00682"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS summing_00682"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE summing_00682(k UInt32, v UInt32) ENGINE SummingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO summing_00682(k, v) VALUES (1, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO summing_00682(k, v) VALUES (1, 2)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE summing_00682 DELETE WHERE k = 1"
wait_for_mutation "summing_00682" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE summing_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM summing_00682"

${CLICKHOUSE_CLIENT} --query="DROP TABLE summing_00682"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS aggregating_00682"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE aggregating_00682(k UInt32, v AggregateFunction(count)) ENGINE AggregatingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO aggregating_00682(k) VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO aggregating_00682(k) VALUES (1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE aggregating_00682 DELETE WHERE k = 1"
wait_for_mutation "aggregating_00682" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE aggregating_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM aggregating_00682"

${CLICKHOUSE_CLIENT} --query="DROP TABLE aggregating_00682"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS replacing_00682"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE replacing_00682(k UInt32, v String) ENGINE ReplacingMergeTree ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO replacing_00682(k, v) VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO replacing_00682(k, v) VALUES (1, 'b')"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE replacing_00682 DELETE WHERE k = 1"
wait_for_mutation "replacing_00682" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE replacing_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM replacing_00682"

${CLICKHOUSE_CLIENT} --query="DROP TABLE replacing_00682"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS collapsing_00682"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE collapsing_00682(k UInt32, v String, s Int8) ENGINE CollapsingMergeTree(s) ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO collapsing_00682(k, v, s) VALUES (1, 'a', 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO collapsing_00682(k, v, s) VALUES (2, 'b', 1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE collapsing_00682 DELETE WHERE k IN (1, 2)"
wait_for_mutation "collapsing_00682" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE collapsing_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM collapsing_00682"

${CLICKHOUSE_CLIENT} --query="DROP TABLE collapsing_00682"


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS versioned_collapsing_00682"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE versioned_collapsing_00682(k UInt32, val String, ver UInt32, s Int8) ENGINE VersionedCollapsingMergeTree(s, ver) ORDER BY k"

${CLICKHOUSE_CLIENT} --query="INSERT INTO versioned_collapsing_00682(k, val, ver, s) VALUES (1, 'a', 0, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO versioned_collapsing_00682(k, val, ver, s) VALUES (2, 'b', 0, 1)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE versioned_collapsing_00682 DELETE WHERE k IN (1, 2)"
wait_for_mutation "versioned_collapsing_00682" "mutation_3.txt"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE versioned_collapsing_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM versioned_collapsing_00682"

${CLICKHOUSE_CLIENT} --query="DROP TABLE versioned_collapsing_00682"
