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

expect_success()
{
    local label="$1"
    shift

    local output
    if ! output=$(${CLICKHOUSE_CLIENT} --send_logs_level=error "$@" 2>&1); then
        echo "FAIL: ${label} should have succeeded:" >&2
        echo "$output" >&2
        exit 1
    fi
    echo "${label}: ${output}"
}

# Waits until a replica falls behind the other one by at least 2 seconds.
wait_stale()
{
    local table="$1"
    local delay_ready=0
    for _ in {1..30}; do
        delay_ready=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.replicas WHERE database = currentDatabase() AND table = '${table}' AND absolute_delay >= 2" 2>/dev/null || true)
        if [[ "$delay_ready" == "1" ]]; then
            return
        fi

        sleep 1
    done

    echo "FAIL: ${table} did not become stale" >&2
    ${CLICKHOUSE_CLIENT} --query="SELECT database, table, absolute_delay, queue_size FROM system.replicas WHERE database = currentDatabase() AND table = '${table}'" >&2 || true
    exit 1
}

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query="SYSTEM START REPLICATION QUEUES" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --query="
        SELECT name FROM system.tables WHERE database = currentDatabase() AND engine IN ('Distributed', 'View', 'MaterializedView')
        ORDER BY engine, name DESC" 2>/dev/null | while read -r table; do
        ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS \`${table}\` SYNC" >/dev/null 2>&1 || true
    done
    ${CLICKHOUSE_CLIENT} -n --query="
        DROP TABLE IF EXISTS mv_target SYNC;
        DROP TABLE IF EXISTS mv_source SYNC;
        DROP TABLE IF EXISTS replicated2 SYNC;
        DROP TABLE IF EXISTS replicated1 SYNC;" >/dev/null 2>&1 || true
}

trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} -n --query="
    CREATE TABLE replicated1 (n UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04256_stale_delay_test', 'r1')
        ORDER BY n;

    CREATE TABLE replicated2 (n UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04256_stale_delay_test', 'r2')
        ORDER BY n;

    CREATE VIEW view_replicated2 AS SELECT * FROM replicated2;

    CREATE TABLE distributed_replicated2 AS replicated2
        ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'replicated2');

    CREATE TABLE distributed_view_replicated2 AS replicated2
        ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'view_replicated2');

    SYSTEM STOP REPLICATION QUEUES replicated2;
    INSERT INTO replicated1 VALUES (1), (2), (3);
    SYSTEM SYNC REPLICA replicated2 PULL;"

wait_stale replicated2

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

# ── Lazy path (stale local view, another replica, fallback to stale allowed) ──
# The shard is read lazily by ReadFromRemote::addLazyPipe, which must accept a view over a
# replicated table. The other replica is this server as well, so it is stale too and the
# query falls back to the stale local replica.

${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE distributed_two_replicas_view_replicated2 AS replicated2
        ENGINE = Distributed('test_cluster_one_shard_two_replicas', currentDatabase(), 'view_replicated2')"

expect_success "[lazy] distributed_two_replicas_view_replicated2" \
    --prefer_localhost_replica=1 \
    --fallback_to_stale_replicas_for_distributed_queries=1 \
    --max_replica_delay_for_distributed_queries=1 \
    --query="SELECT count() FROM distributed_two_replicas_view_replicated2"

# ── Materialized view with a stale inner table and a non-replicated source ───
# Reading a materialized view reads its target, so the stale inner table must be detected
# even though it is not among the referential dependencies of the view.

${CLICKHOUSE_CLIENT} -n --query="
    CREATE TABLE mv_source (n UInt64) ENGINE = MergeTree ORDER BY n;

    -- The other replica of the inner table, created the same way, so that both have the same settings.
    CREATE MATERIALIZED VIEW mv_with_inner_replica1
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04256_stale_delay_test_mv_inner', 'r1')
        ORDER BY n
        AS SELECT n FROM mv_source;

    CREATE MATERIALIZED VIEW mv_with_inner
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04256_stale_delay_test_mv_inner', 'r2')
        ORDER BY n
        AS SELECT n FROM mv_source;

    CREATE TABLE distributed_mv_with_inner AS mv_source
        ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'mv_with_inner');"

inner_table=$(${CLICKHOUSE_CLIENT} --query="SELECT concat('.inner_id.', toString(uuid)) FROM system.tables WHERE database = currentDatabase() AND name = 'mv_with_inner'")

${CLICKHOUSE_CLIENT} -n --query="
    SYSTEM STOP REPLICATION QUEUES \`${inner_table}\`;
    INSERT INTO mv_with_inner_replica1 VALUES (1), (2), (3);
    SYSTEM SYNC REPLICA \`${inner_table}\` PULL;"

wait_stale "${inner_table}"

expect_stale_error "[local] distributed_mv_with_inner" "${local_settings[@]}" --query="SELECT count() FROM distributed_mv_with_inner"
expect_stale_error "[remote] distributed_mv_with_inner" "${remote_settings[@]}" --query="SELECT count() FROM distributed_mv_with_inner"

# ── Materialized view TO a healthy target, populated from a stale source ─────
# The source only populates the target, reading the view does not read it.

${CLICKHOUSE_CLIENT} -n --query="
    CREATE TABLE mv_target (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO mv_target VALUES (1), (2);

    CREATE MATERIALIZED VIEW mv_to_target TO mv_target AS SELECT n FROM replicated2;

    CREATE TABLE distributed_mv_to_target AS mv_target
        ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'mv_to_target');"

expect_success "[local] distributed_mv_to_target" "${local_settings[@]}" --query="SELECT count() FROM distributed_mv_to_target"
expect_success "[remote] distributed_mv_to_target" "${remote_settings[@]}" --query="SELECT count() FROM distributed_mv_to_target"

# ── Views sharing ancestors ──────────────────────────────────────────────────
# Each view reads the two previous ones, so the number of paths to replicated2 grows
# exponentially. Each table must be visited once, otherwise the check never finishes.

${CLICKHOUSE_CLIENT} -n --query="
    CREATE VIEW shared_view_0 AS SELECT n FROM replicated2;
    CREATE VIEW shared_view_1 AS SELECT n FROM replicated2;"

for i in {2..30}; do
    ${CLICKHOUSE_CLIENT} --query="CREATE VIEW shared_view_${i} AS SELECT n FROM shared_view_$((i - 1)) UNION ALL SELECT n FROM shared_view_$((i - 2))"
done

${CLICKHOUSE_CLIENT} --query="
    CREATE TABLE distributed_shared_view AS replicated2
        ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'shared_view_30')"

expect_stale_error "[local] distributed_shared_view" "${local_settings[@]}" --query="SELECT count() FROM distributed_shared_view"
expect_stale_error "[remote] distributed_shared_view" "${remote_settings[@]}" --query="SELECT count() FROM distributed_shared_view"
