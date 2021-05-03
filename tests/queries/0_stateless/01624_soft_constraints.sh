#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS="SET convert_query_to_cnf = 1; SET optimize_using_constraints = 1; SET optimize_move_to_prewhere = 1;"

$CLICKHOUSE_CLIENT -n --query="
DROP DATABASE IF EXISTS constraint_test;
DROP TABLE IF EXISTS constraint_test.test;
CREATE DATABASE constraint_test;
CREATE TABLE constraint_test.test (
  i UInt64,
  a UInt64,
  b UInt64,
  c Float64,
  INDEX t (a < b) TYPE hypothesis GRANULARITY 1,
  INDEX t2 (b <= c) TYPE hypothesis GRANULARITY 1
) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity=1;
"

$CLICKHOUSE_CLIENT --query="INSERT INTO constraint_test.test VALUES
(0, 1, 2, 2),
(1, 2, 1, 2),
(2, 2, 2, 1),
(3, 1, 2, 3)"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM constraint_test.test WHERE b > a FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT --query="SELECT count() FROM constraint_test.test WHERE b <= a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM constraint_test.test WHERE b >= a FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT --query="SELECT count() FROM constraint_test.test WHERE b = a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM constraint_test.test WHERE c < a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM constraint_test.test WHERE c = a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT --query="SELECT count() FROM constraint_test.test WHERE c > a FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT --query="SELECT count() FROM constraint_test.test WHERE c < a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="
DROP TABLE constraint_test.test;
DROP DATABASE constraint_test;"
