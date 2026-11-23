#!/usr/bin/env bash
# Tags: no-parallel, no-async-insert, no-random-merge-tree-settings, no-object-storage, no-encrypted-storage, no-replicated-database, no-parallel-replicas, no-fasttest
# no-async-insert: async inserts compute deduplication block ids differently
# no-fasttest: needs failpoints

# When the logs are replaced by a checkpoint of the entries in the window (see 05336_deduplication_log_bounded_retention),
# a checkpoint that is not complete must not take part in anything: the logs it was to replace stay, and it is not read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The failed checkpoints are logged
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --send_logs_level=fatal"

function churn()
{
    for i in $(seq "$1" "$2")
    do
        $CLICKHOUSE_CLIENT -q "INSERT INTO t_dedup_log_interrupted VALUES ($i)"
        # A block that is deduplicated also takes a block number, so the name of the part is looked up.
        part=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_dedup_log_interrupted' AND active ORDER BY max_block_number DESC LIMIT 1")
        $CLICKHOUSE_CLIENT -q "ALTER TABLE t_dedup_log_interrupted DROP PART '$part'"
    done
}

function check_remembered()
{
    # Blocks 1 and 100 are in the window, together with the block that is inserted and dropped last, so inserting them again must not add rows.
    $CLICKHOUSE_CLIENT --multiquery "
    DETACH TABLE t_dedup_log_interrupted;
    ATTACH TABLE t_dedup_log_interrupted;
    INSERT INTO t_dedup_log_interrupted VALUES (1);
    INSERT INTO t_dedup_log_interrupted VALUES (100);
    SELECT x, count() FROM t_dedup_log_interrupted WHERE x IN (1, 100) GROUP BY x ORDER BY x;
    "
}

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS t_dedup_log_interrupted;
CREATE TABLE t_dedup_log_interrupted (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 3;
SYSTEM STOP MERGES t_dedup_log_interrupted;
INSERT INTO t_dedup_log_interrupted VALUES (1);
SYSTEM ENABLE FAILPOINT deduplication_log_fail_checkpoint;
"

# Every checkpoint fails after it has copied its first entry.
churn 2 16
$CLICKHOUSE_CLIENT -q "INSERT INTO t_dedup_log_interrupted VALUES (100)"
churn 17 32
check_remembered

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT deduplication_log_fail_checkpoint"
churn 33 48
check_remembered

data_path=$($CLICKHOUSE_CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_dedup_log_interrupted'")
logs=$(find "$data_path/deduplication_logs" -type f | wc -l)
[ "$logs" -le 8 ] && echo "bounded" || echo "too many deduplication logs: $logs"
find "$data_path/deduplication_logs" -type f -name '*.tmp' | wc -l

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dedup_log_interrupted"
