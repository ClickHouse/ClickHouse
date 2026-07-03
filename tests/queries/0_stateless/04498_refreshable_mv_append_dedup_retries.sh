#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database
# no-replicated-database: this test explicitly creates a Replicated database. The deduplication
# of APPEND refresh INSERTs across retries applies only to views in a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_r"

wait_for()
{
    for _ in {1..300}
    do
        [ "$(${CLICKHOUSE_CLIENT} -q "$1")" = "1" ] && return
        sleep 0.3
    done
    echo "timed out waiting for: $1"
    ${CLICKHOUSE_CLIENT} -q "SELECT status, exception, retry FROM system.view_refreshes WHERE database = '${db}' AND view = 'mv' FORMAT Vertical"
}

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none <<EOF
CREATE DATABASE ${db} ENGINE = Replicated('/test/{database}/rmv_append_dedup_retries', 'shard1', 'replica1');
CREATE TABLE ${db}.src (x Int64) ENGINE = ReplicatedMergeTree ORDER BY x;
CREATE TABLE ${db}.dst (x Int64) ENGINE = ReplicatedMergeTree ORDER BY x;
INSERT INTO ${db}.src VALUES (1);
EOF

# AFTER 1 YEAR is due right away for a freshly created view (the initial state points at the epoch),
# so the creation itself triggers the one and only refresh occurrence, on the regular schedule path
# where failed attempts are retried.
#
# The refresh emits 1-row blocks (squashing disabled by min_insert_block_size_rows = 1, so block
# boundaries are the same on every attempt), each block spends 0.5s in the filter, and the last
# one throws while `src` is non-empty. Single-threaded execution gives the blocks before it time
# to be committed to `dst` before the refresh fails and is retried.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE MATERIALIZED VIEW ${db}.mv
        REFRESH AFTER 1 YEAR
        SETTINGS refresh_retries = -1, refresh_retry_initial_backoff_ms = 100, refresh_retry_max_backoff_ms = 100
        APPEND TO ${db}.dst
        AS SELECT number AS x FROM numbers(3)
        WHERE throwIf(((number >= 2) AND (SELECT count() > 0 FROM ${db}.src)) + sleepEachRow(0.5) > 0) = 0
        SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1, max_threads = 1, max_insert_threads = 1"

# Wait until a failed refresh attempt has committed at least one block to the target.
wait_for "SELECT count() >= 1 FROM ${db}.dst"

# Make the refresh query succeed. The retried INSERT must be deduplicated against the blocks
# committed by the failed attempts, so every row must end up in the target exactly once.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "TRUNCATE TABLE ${db}.src"

wait_for "SELECT last_success_time IS NOT NULL FROM system.view_refreshes WHERE database = '${db}' AND view = 'mv'"

${CLICKHOUSE_CLIENT} -q "SELECT x FROM ${db}.dst ORDER BY x"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP DATABASE ${db} SYNC"
