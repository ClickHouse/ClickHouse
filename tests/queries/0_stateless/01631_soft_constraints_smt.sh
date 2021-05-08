#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS="SET convert_query_to_cnf = 1; SET optimize_using_constraints = 1; SET optimize_using_smt = 1"

$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
DROP TABLE IF EXISTS hypothesis_test_test;
DROP TABLE IF EXISTS hypothesis_test_test2;
DROP TABLE IF EXISTS hypothesis_test_test3;

CREATE TABLE hypothesis_test_test (
  i UInt64,
  a UInt64,
  b UInt64,
  c Float64,
  INDEX t (a < b) TYPE hypothesis GRANULARITY 1,
  INDEX t2 (b <= c) TYPE hypothesis GRANULARITY 1
) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity=1;
"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;
INSERT INTO hypothesis_test_test VALUES (0, 1, 2, 2), (1, 2, 1, 2), (2, 2, 2, 1), (3, 1, 2, 3)"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test WHERE b > a FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test WHERE b <= a FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test WHERE b >= a FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test WHERE b = a FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test WHERE c < a FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test WHERE c = a FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test WHERE c > a FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test WHERE c < a FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read"


$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
CREATE TABLE hypothesis_test_test2 (
  i UInt64,
  a UInt64,
  b UInt64,
  INDEX t (a != b) TYPE hypothesis GRANULARITY 1
) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity=1;
"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;
INSERT INTO hypothesis_test_test2 VALUES (0, 1, 2), (1, 2, 1), (2, 2, 2), (3, 1, 0)"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test2 WHERE a < b FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test2 WHERE a <= b FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test2 WHERE a = b FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 1

$CLICKHOUSE_CLIENT -n --query="$SETTINGS; SELECT count() FROM hypothesis_test_test2 WHERE a != b FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4


$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
CREATE TABLE hypothesis_test_test3 (
  i UInt64,
  a UInt64,
  b UInt64,
  INDEX t (a = b + 10) TYPE hypothesis GRANULARITY 1
) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity=1;
"

$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
INSERT INTO hypothesis_test_test3 VALUES (0, 1, 2), (1, 2, 1), (2, 12, 2), (3, 1, 0)"

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test_test3 WHERE a < b FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 3

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test_test3 WHERE a > b FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test_test3 WHERE a - 10 <= b FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test_test3 WHERE a - 5 = b + 5 FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 4

$CLICKHOUSE_CLIENT -n --query="$SETTINGS;SELECT count() FROM hypothesis_test_test3 WHERE a - 5 != b + 5 FORMAT JSON SETTINGS optimize_using_smt = 1" | grep "rows_read" # 3


$CLICKHOUSE_CLIENT -n --query="
$SETTINGS;
DROP TABLE hypothesis_test_test;
DROP TABLE hypothesis_test_test2;
DROP TABLE hypothesis_test_test3;"
