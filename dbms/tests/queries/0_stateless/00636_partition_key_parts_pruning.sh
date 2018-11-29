#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT '*** Single column partition key ***'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.single_col_partition_key"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.single_col_partition_key(x UInt32) ENGINE MergeTree ORDER BY x PARTITION BY intDiv(x, 10)"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.single_col_partition_key VALUES (1), (2), (3), (4), (11), (12), (20)"

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.single_col_partition_key WHERE x < 3 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.single_col_partition_key WHERE x >= 11 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.single_col_partition_key WHERE x = 20 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.single_col_partition_key"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Composite partition key ***'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.composite_partition_key"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.composite_partition_key(a UInt32, b UInt32, c UInt32) ENGINE MergeTree ORDER BY c PARTITION BY (intDiv(a, 100), intDiv(b, 10), c)"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test.composite_partition_key VALUES \
    (1, 1, 1), (2, 2, 1), (3, 3, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.composite_partition_key VALUES \
    (100, 10, 2), (101, 11, 2), (102, 12, 2)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.composite_partition_key VALUES \
    (200, 10, 2), (201, 11, 2), (202, 12, 2)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.composite_partition_key VALUES \
    (301, 20, 3), (302, 21, 3), (303, 22, 3)"

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.composite_partition_key WHERE a > 400 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.composite_partition_key WHERE b = 11 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.composite_partition_key WHERE c = 4 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.composite_partition_key WHERE a < 200 AND c = 2 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.composite_partition_key WHERE a = 301 AND b < 20 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.composite_partition_key WHERE b >= 12 AND c = 2 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test.composite_partition_key WHERE a = 301 AND b = 21 AND c = 3 FORMAT XML" | grep -F rows_read | sed 's/^[ \t]*//g'

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.composite_partition_key"
