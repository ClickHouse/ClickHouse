#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS="SET convert_query_to_cnf = 1; SET optimize_using_constraints = 1; SET optimize_move_to_prewhere = 1; SET optimize_substitute_columns = 1; SET optimize_append_index = 1"

$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
DROP DATABASE IF EXISTS hypothesis_test;
DROP TABLE IF EXISTS hypothesis_test.test;
DROP TABLE IF EXISTS hypothesis_test.test2;
DROP TABLE IF EXISTS hypothesis_test.test3;

CREATE DATABASE hypothesis_test;
CREATE TABLE hypothesis_test.test (
  i UInt64,
  a UInt64,
  b UInt64,
  c Float64,
  INDEX t (a < b) TYPE hypothesis GRANULARITY 1,
  INDEX t2 (b <= c) TYPE hypothesis GRANULARITY 1
) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity=1;
"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;
INSERT INTO hypothesis_test.test VALUES (0, 1, 2, 2), (1, 2, 1, 2), (2, 2, 2, 1), (3, 1, 2, 3)"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test WHERE b > a FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test WHERE b <= a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test WHERE b >= a FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test WHERE b = a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test WHERE c < a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test WHERE c = a FORMAT JSON" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test WHERE c > a FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test WHERE c < a FORMAT JSON" | grep "rows_read"


$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
CREATE TABLE hypothesis_test.test2 (
  i UInt64,
  a UInt64,
  b UInt64,
  INDEX t (a != b) TYPE hypothesis GRANULARITY 1
) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity=1;
"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;
INSERT INTO hypothesis_test.test2 VALUES (0, 1, 2), (1, 2, 1), (2, 2, 2), (3, 1, 0)"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test2 WHERE a < b FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test2 WHERE a <= b FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test2 WHERE a = b FORMAT JSON" | grep "rows_read" # 1

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test.test2 WHERE a != b FORMAT JSON" | grep "rows_read" # 4


$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
CREATE TABLE hypothesis_test.test3 (
  i UInt64,
  a UInt64,
  b UInt64,
  INDEX t (a = b) TYPE hypothesis GRANULARITY 1
) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity=1;
"

$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
INSERT INTO hypothesis_test.test3 VALUES (0, 1, 2), (1, 2, 1), (2, 2, 2), (3, 1, 0)"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test.test3 WHERE a < b FORMAT JSON" | grep "rows_read" # 3

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test.test3 WHERE a <= b FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test.test3 WHERE a = b FORMAT JSON" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test.test3 WHERE a != b FORMAT JSON" | grep "rows_read" # 3


$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
DROP TABLE hypothesis_test.test;
DROP TABLE hypothesis_test.test2;
DROP TABLE hypothesis_test.test3;
DROP DATABASE hypothesis_test;"
