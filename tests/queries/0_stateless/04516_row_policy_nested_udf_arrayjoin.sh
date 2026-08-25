#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# arrayJoin can hide behind a chain of SQL UDFs that reference each other. The row policy guard
# must descend through every UDF layer and reject it, otherwise the filter reintroduces the
# 'column->size() == num_rows' logical error at read time.
# SQL UDFs are global (not database-scoped), so the names embed $CLICKHOUSE_DATABASE to avoid
# collisions with concurrent test runs.

$CLICKHOUSE_CLIENT -q "
  DROP ROW POLICY IF EXISTS nested_udf_policy ON row_policy_nested_udf_table;
  DROP ROW POLICY IF EXISTS nested_udf_unnest_policy ON row_policy_nested_udf_table;
  DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_nested_inner;
  DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_nested_outer;
  DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_nested_inner_unnest;
  DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_nested_outer_unnest;
  DROP TABLE IF EXISTS row_policy_nested_udf_table;

  CREATE TABLE row_policy_nested_udf_table (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id;
  INSERT INTO row_policy_nested_udf_table VALUES (1, 10), (2, 20);

  -- A UDF wrapping another UDF that expands to arrayJoin is rejected (recursive descent, two levels).
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_nested_inner AS (y) -> (arrayJoin([1, 2]) OR y = 0);
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_nested_outer AS (x) -> (${CLICKHOUSE_DATABASE}_nested_inner(x));
  CREATE ROW POLICY nested_udf_policy ON row_policy_nested_udf_table FOR SELECT USING ${CLICKHOUSE_DATABASE}_nested_outer(value) TO ALL; -- { serverError ILLEGAL_PREWHERE }

  -- Same chain but the arrayJoin is spelled via its case-insensitive alias unnest deep inside.
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_nested_inner_unnest AS (y) -> (unnest([1, 2]) OR y = 0);
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_nested_outer_unnest AS (x) -> (${CLICKHOUSE_DATABASE}_nested_inner_unnest(x));
  CREATE ROW POLICY nested_udf_unnest_policy ON row_policy_nested_udf_table FOR SELECT USING ${CLICKHOUSE_DATABASE}_nested_outer_unnest(value) TO ALL; -- { serverError ILLEGAL_PREWHERE }

  -- The table is still readable (no policy was actually created).
  SELECT * FROM row_policy_nested_udf_table ORDER BY id;

  DROP FUNCTION ${CLICKHOUSE_DATABASE}_nested_outer;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_nested_inner;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_nested_outer_unnest;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_nested_inner_unnest;
  DROP TABLE row_policy_nested_udf_table;
"
