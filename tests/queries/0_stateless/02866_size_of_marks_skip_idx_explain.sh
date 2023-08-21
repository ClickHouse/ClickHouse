#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --optimize_move_to_prewhere=1 --convert_query_to_cnf=0 --optimize_read_in_order=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_skip_idx"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_skip_idx (id UInt32, INDEX name_idx_g2 id TYPE minmax GRANULARITY 2, INDEX name_idx_g1 id TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_skip_idx SELECT number FROM system.numbers LIMIT 5 OFFSET 1"

$CLICKHOUSE_CLIENT -q "
    EXPLAIN indexes = 1 SELECT * FROM test_skip_idx WHERE id < 2
    " | grep -A 100 "ReadFromMergeTree" # | grep -v "Description"

echo "-----------------"

$CLICKHOUSE_CLIENT -q "
    EXPLAIN indexes = 1 SELECT * FROM test_skip_idx WHERE id < 3
    " | grep -A 100 "ReadFromMergeTree" # | grep -v "Description"
