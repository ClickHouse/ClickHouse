#!/usr/bin/env bash
# Test: concurrent vertical inserts are safe and produce consistent results.

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_vi_concurrent"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_vi_concurrent
(
    k UInt64,
    v String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;"

for i in 0 1 2 3; do
  $CLICKHOUSE_CLIENT --query "INSERT INTO t_vi_concurrent SELECT number + ${i} * 1000, toString(number) FROM numbers(1000)" &
done
wait

$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_vi_concurrent"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_vi_concurrent"
