#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# Tag no-fasttest: `S3Queue` is not compiled into the fast-test build.
# Tag no-parallel: enables a fail point, which is server-global state.
# Tag no-replicated-database: the table would exist on two replicas sharing one Keeper path,
# so the active registry would not become empty after `SYSTEM STOP` on one of them.
#
# A paused `S3Queue` table removes its entry from the active registry. If that removal fails
# because of a transient Keeper error, the next paused poll must retry it: an entry left behind
# would make the abandoned-claim cleanup treat every claim of this table as owned by a live
# consumer for as long as the table stays paused.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
TABLE="s3queue_retry_unregister_${DB}"
KEEPER_PATH="/clickhouse/test/${DB}/s3queue_retry_unregister"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT object_storage_queue_fail_unregister_active"
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS ${TABLE}_mv SYNC;
        DROP TABLE IF EXISTS ${TABLE}_dst SYNC;
        DROP TABLE IF EXISTS ${TABLE} SYNC;
    "
}
trap cleanup EXIT

function registry_entries()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '${KEEPER_PATH}/registry'"
}

function wait_for_registry_entries()
{
    local expected=$1
    for _ in $(seq 1 120); do
        if [ "$(registry_entries)" == "$expected" ]; then
            echo "registry entries: $expected"
            return
        fi
        sleep 0.5
    done
    echo "registry entries: $(registry_entries), expected $expected"
}

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${TABLE}_mv SYNC;
    DROP TABLE IF EXISTS ${TABLE}_dst SYNC;
    DROP TABLE IF EXISTS ${TABLE} SYNC;

    CREATE TABLE ${TABLE} (x UInt64)
    ENGINE = S3Queue('http://localhost:11111/test/${DB}_s3queue_retry_unregister/*', 'test', 'testtest', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}',
        polling_min_timeout_ms = 100, polling_max_timeout_ms = 1000, polling_backoff_ms = 100;

    CREATE TABLE ${TABLE}_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW ${TABLE}_mv TO ${TABLE}_dst AS SELECT x FROM ${TABLE};
"

# With an attached view the table registers itself as an active consumer. Polling more often
# than every 5 seconds, it keeps the entry even while there is nothing to process.
wait_for_registry_entries 1

# The first removal attempt after the pause fails; paused polls repeat every 5 seconds.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT object_storage_queue_fail_unregister_active"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP ${TABLE}"

wait_for_registry_entries 0

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'unregister failed once: ' || toString(count() >= 1)
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND logger_name LIKE '%${TABLE}%' AND message LIKE '%Failed to unregister from the active registry%'
"
