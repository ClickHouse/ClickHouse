#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS restoration_destination SYNC;
        DROP TABLE IF EXISTS restoration_source SYNC;
    "
}
trap cleanup EXIT
cleanup

zk_path="/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE restoration_source (x UInt64)
    ENGINE = ReplicatedMergeTree('${zk_path}', 'source') ORDER BY x;
    CREATE TABLE restoration_destination (x UInt64)
    ENGINE = ReplicatedMergeTree('${zk_path}', 'destination') ORDER BY x
    SETTINGS max_postpone_time_for_failed_replicated_tasks_ms = 0;
    SYSTEM STOP MERGES restoration_source;
    INSERT INTO restoration_source VALUES (111);
    INSERT INTO restoration_source VALUES (222);
    INSERT INTO restoration_source VALUES (333);
    SYSTEM START MERGES restoration_source;
    OPTIMIZE TABLE restoration_source FINAL;
    SYSTEM SYNC REPLICA restoration_destination;
    SYSTEM STOP MERGES restoration_source;
    SYSTEM STOP MERGES restoration_destination;
"
old_part=$(${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'restoration_source' AND active")

# Retain local data while removing Keeper metadata through supported replica removal.
${CLICKHOUSE_CLIENT} --multiquery --query "
    DETACH TABLE restoration_destination;
    DETACH TABLE restoration_source;
    SYSTEM DROP REPLICA 'destination' FROM ZKPATH '${zk_path}';
    SYSTEM DROP REPLICA 'source' FROM ZKPATH '${zk_path}';
    ATTACH TABLE restoration_source;
    ATTACH TABLE restoration_destination;
"
readonly=0
for _ in $(seq 1 120); do
    readonly=$(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(is_readonly) FROM system.replicas
        WHERE database = currentDatabase() AND table IN ('restoration_source', 'restoration_destination')")
    if [[ "$readonly" == 2 ]]; then
        break
    fi
    sleep 0.1
done
[[ "$readonly" == 2 ]]

# The first restoration allocates a new identity; the other replica has the old detached name.
${CLICKHOUSE_CLIENT} --multiquery --query "
    SYSTEM RESTORE REPLICA restoration_source;
    SYSTEM SYNC REPLICA restoration_source;
    SYSTEM STOP REPLICATED SENDS restoration_source;
    SYSTEM STOP FETCHES restoration_destination;
    SYSTEM RESTORE REPLICA restoration_destination;
"
queue_ready=0
for _ in $(seq 1 120); do
    queue_ready=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() = 1 AND countIf(type = 'ATTACH_PART' AND empty(source_replica) AND NOT is_currently_executing) = 1
        FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 'restoration_destination'")
    if [[ "$queue_ready" == 1 ]]; then
        break
    fi
    sleep 0.1
done
[[ "$queue_ready" == 1 ]]
printf 'legacy_queue\t1\n'
${CLICKHOUSE_CLIENT} --multiquery --query "
    SYSTEM START FETCHES restoration_destination;
    SYSTEM SYNC REPLICA restoration_destination;
    SELECT 'data', groupArray(x) FROM (SELECT x FROM restoration_destination ORDER BY x);
    SELECT 'renamed_identity', name != '${old_part}'
    FROM system.parts WHERE database = currentDatabase() AND table = 'restoration_destination' AND active;
    SELECT 'old_seed_consumed', count() = 0 FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 'restoration_destination' AND name = '${old_part}';
"
