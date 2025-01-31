#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=/home/nikita/ClickHouse/ClickHouse/programs/server/user_files/${CLICKHOUSE_DATABASE}_db1

sqlite3 "${DB_PATH}" 'DROP TABLE IF EXISTS t1'
sqlite3 "${DB_PATH}" 'CREATE TABLE t1(c0 INT,c1 INT);'

chmod ugo+w "${DB_PATH}"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE ${CLICKHOUSE_DATABASE}.t0 (c0 Int) ENGINE = Memory();"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ${CLICKHOUSE_DATABASE}.t1 (c0 Int, c1 Int) ENGINE = SQLite('${DB_PATH}', 't1');";
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ${CLICKHOUSE_DATABASE}.t2 (c0 Int, c1 Int) ENGINE = Memory();";
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE ${CLICKHOUSE_DATABASE}.t0 (c0) VALUES (1);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE ${CLICKHOUSE_DATABASE}.t1 (c0, c1) VALUES (-3, 0), (1, 0), (-2, 0);";
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE ${CLICKHOUSE_DATABASE}.t2 (c0, c1) VALUES (-3, 0), (1, 0), (-2, 0);";

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM ${CLICKHOUSE_DATABASE}.t0 tx RIGHT JOIN ${CLICKHOUSE_DATABASE}.t1 ty ON ty.c1 = tx.c0 WHERE tx.c0 < 1;"

# That's what I see in logs. The query: SELECT "c0", "c1" FROM "t1" WHERE "c0" < 1 to the SQLite seems incorrect. It in fact did a filter push down to a table t1
# when the filter should have applied to a table t0.
# This happens because `transformQueryForExternalDatabase` function just copies WHERE from the original query.
#
# 2025.01.31 03:02:48.867647 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Debug> executeQuery: (from [::1]:39400) (comment: 03325_sqlite_join_wrong_answer.sh) (query 1, line 1) SELECT * FROM test_i4qsz34e.t0 tx RIGHT JOIN test_i4qsz34e.t1 ty ON ty.c1 = tx.c0 WHERE tx.c0 < 1; (stage: Complete)
# 2025.01.31 03:02:48.868068 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Trace> Planner: Query to stage Complete
# 2025.01.31 03:02:48.868203 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Trace> StorageSQLite (t1): Query: SELECT "c0", "c1" FROM "t1" WHERE "c0" < 1
# 2025.01.31 03:02:48.868317 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Trace> HashJoin: Keys: [(__table1.c0) = (__table2.c1)], datatype: EMPTY, kind: Right, strictness: All, right header: __table2.c0 Int32 Int32(size = 0), __table2.c1 Int32 Int32(size = 0)
# 2025.01.31 03:02:48.868352 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Trace> JoiningTransform: Before join block: '__table1.c0 Int32 Int32(size = 0)'
# 2025.01.31 03:02:48.868386 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Trace> JoiningTransform: After join block: '__table1.c0 Int32 Int32(size = 0), __table2.c0 Int32 Int32(size = 0), __table2.c1 Int32 Int32(size = 0)'
# 2025.01.31 03:02:48.868408 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Trace> Planner: Query from stage FetchColumns to stage Complete
# 2025.01.31 03:02:48.868535 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Trace> optimizeJoinLegacy: Left table estimation: 1, right table estimation: unknown
# 2025.01.31 03:02:48.869208 [ 43666 ] {637628db-86fe-4f14-a04a-a4716189bab2} <Debug> executeQuery: Read 3 rows, 20.00 B in 0.001732 sec., 1732.1016166281754 rows/sec., 11.28 KiB/sec.

echo "-----"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM ${CLICKHOUSE_DATABASE}.t0 tx RIGHT JOIN ${CLICKHOUSE_DATABASE}.t2 ty ON ty.c1 = tx.c0 WHERE tx.c0 < 1;"
