#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FUNCTION_NAME="function_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "
  DROP FUNCTION IF EXISTS $FUNCTION_NAME;
  DROP TABLE IF EXISTS t;
"

$CLICKHOUSE_CLIENT -q "
  CREATE FUNCTION $FUNCTION_NAME AS (x) -> (x AS c0);
  CREATE TABLE t (t0 UInt64, t1 UInt64) ENGINE=MergeTree() ORDER BY t0;
"

$CLICKHOUSE_CLIENT -q "SELECT 1 FROM (SELECT 1 AS x) x ARRAY JOIN $FUNCTION_NAME(x.x); -- { serverError TYPE_MISMATCH, ALIAS_REQUIRED }"
$CLICKHOUSE_CLIENT -q "SELECT number from numbers(1) array join *; -- { serverError TYPE_MISMATCH, ALIAS_REQUIRED }"
$CLICKHOUSE_CLIENT -q "SELECT number from numbers(1) array join 1,*; -- { serverError TYPE_MISMATCH, ALIAS_REQUIRED }"

$CLICKHOUSE_CLIENT -q "
  DROP FUNCTION IF EXISTS $FUNCTION_NAME;
  DROP TABLE IF EXISTS t;
"