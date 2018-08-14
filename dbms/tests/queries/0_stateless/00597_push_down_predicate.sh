#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

exception_pattern="Code: 277.*is not used and setting 'force_primary_key' is set.."

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_union_1;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_union_2;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_join_1;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_join_2;"


${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.test(date Date, id Int8, name String, value Int64) ENGINE = MergeTree(date, (id, date), 8192);"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.test_union_1(date_1 Date, id_1 Int8, name_1 String, value_1 Int64) ENGINE = MergeTree(date_1, (id_1, date_1), 8192);"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.test_union_2(date_2 Date, id_2 Int8, name_2 String, value_2 Int64) ENGINE = MergeTree(date_2, (id_2, date_2), 8192);"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.test_join_1(date_1 Date, id_1 Int8, name_1 String, value_1 Int64) ENGINE = MergeTree(date_1, (id_1, date_1), 8192);"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.test_join_2(date_2 Date, id_2 Int8, name_2 String, value_2 Int64) ENGINE = MergeTree(date_2, (id_2, date_2), 8192);"


${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test VALUES('2000-01-01', 1, 'test string 1', 1);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test VALUES('2000-01-01', 2, 'test string 2', 2);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_union_1 VALUES('2000-01-01', 1, 'test string 1', 1);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_union_1 VALUES('2000-01-01', 2, 'test string 2', 2);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_union_2 VALUES('2000-01-01', 1, 'test string 1', 1);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_union_2 VALUES('2000-01-01', 2, 'test string 2', 2);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_join_1 VALUES('2000-01-01', 1, 'test string 1', 1);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_join_1 VALUES('2000-01-01', 2, 'test string 2', 2);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_join_2 VALUES('2000-01-01', 1, 'test string 1', 1);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test.test_join_2 VALUES('2000-01-01', 2, 'test string 2', 2);"

# Queries that previously worked but now don't work.
echo `${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression 1 --query 'SELECT * FROM (SELECT 1) WHERE \`1\` = 1;' 2>&1 | grep -c "Unknown identifier: 1."`

# Not need push down, but it works.
${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression 1 --query "SELECT 1;"
${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression 1 --query "SELECT 1 AS id WHERE id = 1;"
${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression 1 --query "SELECT arrayJoin([1,2,3]) AS id WHERE id = 1;"
${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression 1 --query "SELECT * FROM (SELECT * FROM test.test) WHERE id = 1;"

# Need push down
${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression 1 --query "SELECT * FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;"
${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression 1 --query "SELECT id FROM (SELECT arrayJoin([1, 2, 3]) AS id) WHERE id = 1;"
${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression 1 --query "SELECT date, id, name, value FROM (SELECT date, name, value,min(id) AS id FROM test.test GROUP BY date, name, value) WHERE id = 1;"
${CLICKHOUSE_CLIENT} --force_primary_key 1 --enable_optimize_predicate_expression 1 --query "SELECT date, id, name, value FROM (SELECT date, id, name, value FROM test.test) WHERE id = 1;"
${CLICKHOUSE_CLIENT} --force_primary_key 1 --enable_optimize_predicate_expression 1 --query "SELECT date, id FROM (SELECT id, date, min(value) FROM test.test GROUP BY id, date) WHERE id = 1;"
${CLICKHOUSE_CLIENT} --force_primary_key 1 --enable_optimize_predicate_expression 1 --query "SELECT date_1, id_1, name_1, value_1 FROM (SELECT date_1, id_1, name_1, value_1 FROM test.test_union_1 UNION ALL SELECT date_2, id_2, name_2, value_2 FROM test.test_union_2) WHERE id_1 = 1;"
${CLICKHOUSE_CLIENT} --force_primary_key 1 --enable_optimize_predicate_expression 1 --query "SELECT * FROM (SELECT id_1, name_1 AS name FROM test.test_join_1) ANY LEFT JOIN (SELECT id_2, name_2 AS name FROM test.test_join_2) USING name WHERE id_1 = 1 AND id_2 = 1;"
${CLICKHOUSE_CLIENT} --force_primary_key 1 --enable_optimize_predicate_expression 1 --query "SELECT * FROM (SELECT id_1, name_1 AS name FROM test.test_join_1) ANY LEFT JOIN (SELECT id_2, name_2 AS name FROM test.test_union_2 UNION ALL SELECT id_1, name_1 AS name FROM test.test_union_1) USING name WHERE id_1 = 1 AND id_2 = 1;"
${CLICKHOUSE_CLIENT} --force_primary_key 1 --enable_optimize_predicate_expression 1 --query "SELECT * FROM (SELECT name_1,id_1 AS id_1, id_1 AS id_2 FROM test.test_union_1 UNION ALL (SELECT name,id_1,id_2 FROM (SELECT name_1 AS name, id_1 FROM test.test_join_1) ANY INNER JOIN (SELECT  name_2 AS name, id_2 FROM test.test_join_2) USING (name))) WHERE id_1 = 1 AND id_2 = 1;"
echo `${CLICKHOUSE_CLIENT} --force_primary_key 1 --enable_optimize_predicate_expression 1 --query "SELECT * FROM (SELECT * FROM test.test) WHERE id = 1;" 2>&1 | grep -c "$exception_pattern"`
echo `${CLICKHOUSE_CLIENT} --force_primary_key 1 --enable_optimize_predicate_expression 1 --query "SELECT id FROM (SELECT min(id) AS id FROM test.test) WHERE id = 1;" 2>&1 | grep -c "$exception_pattern"`

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_union_1;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_union_2;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_join_1;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.test_join_2;"
