#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ReadFromMerge::RowPolicyData` compiles the row policy condition as a standalone expression, so
# there is no surrounding query plan to evaluate a scalar subquery later and the helper executes it
# up front. A scalar subquery can also hide inside a SQL UDF body, and the Analyzer inlines SQL UDFs
# only while resolving - after that execution step - so the helper must inline them first. Otherwise
# the policy filters against an unevaluated subquery and the read returns too few rows.
# SQL UDFs are global (not database-scoped), so the names embed $CLICKHOUSE_DATABASE to avoid
# collisions with concurrent test runs.

$CLICKHOUSE_CLIENT -q "
  CREATE TABLE limits (v UInt64) ENGINE = MergeTree ORDER BY v;
  INSERT INTO limits VALUES (3);

  CREATE TABLE data (x UInt64) ENGINE = MergeTree ORDER BY x;
  INSERT INTO data SELECT number FROM numbers(6);

  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_below_limit AS (x) -> (x <= (SELECT max(v) FROM limits));
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_below_const AS (x) -> (x <= 3);

  SELECT 'scalar subquery hidden in a UDF';
  CREATE ROW POLICY p ON data USING ${CLICKHOUSE_DATABASE}_below_limit(x) TO ALL;
  SELECT x FROM merge(currentDatabase(), '^data\$') ORDER BY x;
  DROP ROW POLICY p ON data;

  SELECT 'the same scalar subquery spelled out';
  CREATE ROW POLICY p ON data USING x <= (SELECT max(v) FROM limits) TO ALL;
  SELECT x FROM merge(currentDatabase(), '^data\$') ORDER BY x;
  DROP ROW POLICY p ON data;

  SELECT 'a UDF without a subquery';
  CREATE ROW POLICY p ON data USING ${CLICKHOUSE_DATABASE}_below_const(x) TO ALL;
  SELECT x FROM merge(currentDatabase(), '^data\$') ORDER BY x;
  DROP ROW POLICY p ON data;

  DROP FUNCTION ${CLICKHOUSE_DATABASE}_below_limit;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_below_const;
  DROP TABLE data;
  DROP TABLE limits;
"
