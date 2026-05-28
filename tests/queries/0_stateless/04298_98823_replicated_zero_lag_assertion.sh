#!/usr/bin/env bash
# Tags: zookeeper, no-parallel

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/98823.
#
# In `initializeReplication`, the post-recovery unsynced check was
#     max_log_ptr + max_replication_lag_to_enqueue <= new_max_log_ptr
# With `max_replication_lag_to_enqueue` set to `0` the condition becomes
#     max_log_ptr <= new_max_log_ptr
# which is always true (max_log_ptr in ZooKeeper is monotonically non-decreasing).
# A freshly-recovered, fully caught-up replica was therefore mis-marked as
# unsynced, and the next DDL would trip `chassert(our_log_ptr < max_log_ptr)`
# in `initAndCheckTask`, aborting the server in debug and sanitizer builds.
#
# With the fix the comparison is strict-less-than, so a caught-up replica
# is correctly reported as synced and `system.clusters.unsynced_after_recovery`
# is `0`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="rdb_zero_lag_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db} SYNC"

# Create the Replicated database with the zero replication-lag threshold that
# triggered the assertion in the original report. The fresh path means the
# replica's recovery completes with our_log_ptr == max_log_ptr (caught up).
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${db}
    ENGINE = Replicated('/test/${CLICKHOUSE_DATABASE}/rdb_zero_lag', 's1', 'r1')
    SETTINGS max_replication_lag_to_enqueue = 0
"

# Wait until the DDL worker has finished `initializeReplication`. The active
# node in ZooKeeper is created at the end of that function, so once
# `is_active = 1` the in-memory `unsynced_after_recovery` flag is set.
for _ in $(seq 1 100); do
    is_active=$(${CLICKHOUSE_CLIENT} -q "SELECT is_active FROM system.clusters WHERE cluster = '${db}' LIMIT 1")
    if [ "${is_active}" = "1" ]; then
        break
    fi
    sleep 0.1
done

# For a caught-up replica `unsynced_after_recovery` must be 0. On the buggy
# master it is 1 because the comparison was non-strict.
${CLICKHOUSE_CLIENT} -q "
    SELECT unsynced_after_recovery
    FROM system.clusters
    WHERE cluster = '${db}'
"

# Also exercise the post-recovery DDL path: with the bug, scheduling any new
# task would hit `chassert(our_log_ptr < max_log_ptr)` in `initAndCheckTask`
# during the race between entry creation and `max_log_ptr` update. With the
# fix the assertion is `<=` and the path is harmless. INSERT exercises the
# DDL queue, then SELECT confirms the server is alive and serving queries.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "CREATE TABLE ${db}.t (n Int) ENGINE = MergeTree ORDER BY n"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${db}.t VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${db}.t"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db} SYNC"
