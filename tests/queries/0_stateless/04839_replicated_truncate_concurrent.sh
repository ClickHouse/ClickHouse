#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t SYNC;
    CREATE TABLE t (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') PARTITION BY k % 4 ORDER BY k;
    INSERT INTO t SELECT number FROM numbers(1000);
"

# A replicated truncate only schedules the removal, so truncates of the same table must tolerate each other.
pids=()
for _ in {1..4}
do
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t PARALLEL WITH TRUNCATE TABLE t" &
    pids+=($!)
done

for pid in "${pids[@]}"
do
    wait "$pid" || echo "concurrent truncate failed"
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM t"

# The table is still usable afterwards.
$CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT number FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t"
