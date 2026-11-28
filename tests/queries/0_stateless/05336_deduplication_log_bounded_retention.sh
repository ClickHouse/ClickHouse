#!/usr/bin/env bash
# Tags: no-async-insert, no-random-merge-tree-settings, no-object-storage, no-encrypted-storage, no-replicated-database, no-parallel-replicas
# no-async-insert: async inserts compute deduplication block ids differently

# A table that keeps inserting and dropping parts writes an ADD and a DROP record per part to the deduplication log.
# They cancel each other out, so they never fill `non_replicated_deduplication_window`, and the old logs used to stay forever.
# The logs must stay bounded, and the block that is still in the window must be remembered.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS t_dedup_log_churn;
CREATE TABLE t_dedup_log_churn (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 2;
SYSTEM STOP MERGES t_dedup_log_churn;
INSERT INTO t_dedup_log_churn VALUES (1);
"

for i in $(seq 2 60)
do
    $CLICKHOUSE_CLIENT --multiquery "
    INSERT INTO t_dedup_log_churn VALUES ($i);
    ALTER TABLE t_dedup_log_churn DROP PART 'all_${i}_${i}_0';
    "
done

data_path=$($CLICKHOUSE_CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_dedup_log_churn'")
logs=$(find "$data_path/deduplication_logs" -type f | wc -l)
# 118 records in 4-record logs are about 30 files without the fix.
[ "$logs" -le 8 ] && echo "bounded" || echo "too many deduplication logs: $logs"

# Block 1 is still in the window and has to be remembered after the logs are read again.
$CLICKHOUSE_CLIENT --multiquery "
DETACH TABLE t_dedup_log_churn;
ATTACH TABLE t_dedup_log_churn;
INSERT INTO t_dedup_log_churn VALUES (1);
SELECT count() FROM t_dedup_log_churn WHERE x = 1;
DROP TABLE t_dedup_log_churn;
"
