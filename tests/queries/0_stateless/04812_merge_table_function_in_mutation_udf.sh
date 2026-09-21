#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The body of a SQL user-defined function is inlined before the database is filled in, so an
# unqualified table in the body is resolved in the database of the updated table as well.
# The name of a user-defined function is global, not scoped to a database, so it is built from the
# name of the test database to keep concurrent runs of this test independent.
$CLICKHOUSE_CLIENT -q "
  CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE_1};

  CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t_lwu (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
      SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
  INSERT INTO ${CLICKHOUSE_DATABASE_1}.t_lwu VALUES (1, 0), (2, 0), (3, 0);

  -- The source table exists in both databases with a different row, so an expression resolved in the
  -- database of the session silently updates a different row instead of failing.
  CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t_lwu_src (id UInt64) ENGINE = MergeTree ORDER BY id;
  INSERT INTO ${CLICKHOUSE_DATABASE_1}.t_lwu_src VALUES (2);
  CREATE TABLE t_lwu_src (id UInt64) ENGINE = MergeTree ORDER BY id;
  INSERT INTO t_lwu_src VALUES (3);

  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_udf_lwu_src AS () -> (SELECT max(id) FROM t_lwu_src);
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_udf_lwu_in_src AS (x) -> (x IN (SELECT id FROM t_lwu_src));

  -- The whole row is printed after every statement, so a read-back also fails if the statement does
  -- not update any row at all.
  UPDATE ${CLICKHOUSE_DATABASE_1}.t_lwu
      SET v = 7 WHERE id IN (SELECT id FROM t_lwu_src WHERE id = ${CLICKHOUSE_DATABASE}_udf_lwu_src());
  SELECT id, v FROM ${CLICKHOUSE_DATABASE_1}.t_lwu ORDER BY id;

  UPDATE ${CLICKHOUSE_DATABASE_1}.t_lwu
      SET v = ${CLICKHOUSE_DATABASE}_udf_lwu_src() + 10 WHERE id = 1;
  SELECT id, v FROM ${CLICKHOUSE_DATABASE_1}.t_lwu ORDER BY id;

  -- The function call is the whole predicate here, so it is the expression that the inlining replaces.
  UPDATE ${CLICKHOUSE_DATABASE_1}.t_lwu
      SET v = 8 WHERE ${CLICKHOUSE_DATABASE}_udf_lwu_in_src(id);
  SELECT id, v FROM ${CLICKHOUSE_DATABASE_1}.t_lwu ORDER BY id;

  DROP FUNCTION ${CLICKHOUSE_DATABASE}_udf_lwu_src;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_udf_lwu_in_src;

  DROP TABLE t_lwu_src;
  DROP DATABASE ${CLICKHOUSE_DATABASE_1};
"
