#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_for_renames0"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_for_renames50"


$CLICKHOUSE_CLIENT --query "CREATE TABLE table_for_renames0 (value UInt64, data String) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/concurrent_rename', '1') ORDER BY tuple() SETTINGS cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, min_rows_for_compact_part = 100000, min_rows_for_compact_part = 10000000, write_ahead_log_max_bytes = 1"


$CLICKHOUSE_CLIENT --query "INSERT INTO table_for_renames0 SELECT number, toString(number) FROM numbers(1000)"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_for_renames0 SELECT number, toString(number) FROM numbers(1000, 1000)"

$CLICKHOUSE_CLIENT --query "INSERT INTO table_for_renames0 SELECT number, toString(number) FROM numbers(2000, 1000)"

for i in $(seq 1 50); do
    prev_i=$((i - 1))
    $CLICKHOUSE_CLIENT --query "RENAME TABLE table_for_renames$prev_i TO table_for_renames$i"
done

$CLICKHOUSE_CLIENT --query "SELECT COUNT() from table_for_renames50"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_for_renames0"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_for_renames50"
