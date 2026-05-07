#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -u

local_settings=(
    --prefer_localhost_replica=1
    --fallback_to_stale_replicas_for_distributed_queries=0
    --max_replica_delay_for_distributed_queries=1)

remote_settings=(
    --prefer_localhost_replica=0
    --fallback_to_stale_replicas_for_distributed_queries=0
    --max_replica_delay_for_distributed_queries=1)

expect_stale_error()
{
    local label="$1"
    shift

    local output
    if output=$(${CLICKHOUSE_CLIENT} "$@" 2>&1); then
        echo "FAIL: ${label} should have raised ALL_REPLICAS_ARE_STALE" >&2
        exit 1
    fi

    if ! grep -q -E "ALL_REPLICAS_ARE_STALE|Code: 369" <<< "$output"; then
        echo "FAIL: ${label} raised an unexpected error:" >&2
        echo "$output" >&2
        exit 1
    fi
}

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="SYSTEM START REPLICATION QUEUES replicated2" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -n --query="
        DROP TABLE IF EXISTS distributed_view_replicated2 SYNC;
        DROP TABLE IF EXISTS distributed_replicated2 SYNC;
        DROP VIEW IF EXISTS view_replicated2 SYNC;
        DROP TABLE IF EXISTS replicated2 SYNC;
        DROP TABLE IF EXISTS replicated1 SYNC;" >/dev/null 2>&1 || true
}

trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} -n --query="
    CREATE TABLE replicated1 (n UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03734_stale_delay_test', 'r1')
        ORDER BY n;

    CREATE TABLE replicated2 (n UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03734_stale_delay_test', 'r2')
        ORDER BY n;

    CREATE VIEW view_replicated2 AS SELECT * FROM replicated2;

    CREATE TABLE distributed_replicated2 AS replicated2
        ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'replicated2');

    CREATE TABLE distributed_view_replicated2 AS replicated2
        ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'view_replicated2');

    SYSTEM STOP REPLICATION QUEUES replicated2;
    INSERT INTO replicated1 VALUES (1), (2), (3);
    SYSTEM SYNC REPLICA replicated2 PULL;"

delay_ready=0
for _ in {1..30}; do
    delay_ready=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.replicas WHERE database = currentDatabase() AND table = 'replicated2' AND absolute_delay >= 2" 2>/dev/null || true)
    if [[ "$delay_ready" == "1" ]]; then
        break
    fi

    sleep 1
done

if [[ "$delay_ready" != "1" ]]; then
    echo "FAIL: replicated2 did not become stale" >&2
    ${CLICKHOUSE_CLIENT} --query="SELECT database, table, absolute_delay, queue_size FROM system.replicas WHERE database = currentDatabase() AND table = 'replicated2'" >&2 || true
    exit 1
fi

# ── Local path (prefer_localhost_replica=1) ──────────────────────────────────
# The executing node is itself a replica; delay check happens in
# SelectStreamFactory::createForShardImpl.

expect_stale_error "[local] distributed_replicated2" "${local_settings[@]}" --query="SELECT * FROM distributed_replicated2"
expect_stale_error "[local] distributed_view_replicated2" "${local_settings[@]}" --query="SELECT count() FROM distributed_view_replicated2"

# ── Remote path (prefer_localhost_replica=0) ─────────────────────────────────
# The coordinator asks the replica for its table status via TablesStatusRequest;
# delay check happens in TCPHandler::processTablesStatusRequest.

expect_stale_error "[remote] distributed_replicated2" "${remote_settings[@]}" --query="SELECT * FROM distributed_replicated2"
expect_stale_error "[remote] distributed_view_replicated2" "${remote_settings[@]}" --query="SELECT count() FROM distributed_view_replicated2"
