#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database
# Tag no-parallel: uses failpoints, which affect the whole server.
# Tag no-replicated-database: the durable metadata commit lives in ZooKeeper there and follows a different path.

# A local `ALTER TABLE ... MODIFY SETTING` applies the new settings in memory before it writes the durable
# metadata and rolls them back when that write throws. `persist_mutation_author` gates the format of the
# shared `/mutations` entries, so a mutation that starts inside that window must not read the transient
# value: `StorageReplicatedMergeTree::mutate` has to wait for the `ALTER` lock, like `StorageMergeTree::mutate`
# does. Otherwise a failed enabling `ALTER` would still leave a `format version: 2` entry in ZooKeeper.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TABLE=t_mutation_waits_for_settings_alter
ZK_PATH="/clickhouse/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${TABLE}"
ALTER_QUERY_ID="alter_settings_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
MUTATION_QUERY_ID="mutation_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
ALTER_OUTPUT="${CLICKHOUSE_TMP}/${ALTER_QUERY_ID}.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT alter_settings_pause_before_metadata_write" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT alter_settings_throw_before_metadata_write" 2>/dev/null || true
    wait || true
    rm -f "$ALTER_OUTPUT"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $TABLE (id UInt64, value String) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$TABLE', '1') ORDER BY id"
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE VALUES (1, 'a')"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT alter_settings_pause_before_metadata_write"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT alter_settings_throw_before_metadata_write"

# The ALTER parks right before the durable metadata write, with the setting already applied in memory.
$CLICKHOUSE_CLIENT --query-id="$ALTER_QUERY_ID" --query "ALTER TABLE $TABLE MODIFY SETTING persist_mutation_author = 1" > "$ALTER_OUTPUT" 2>&1 &
ALTER_PID=$!

timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT alter_settings_pause_before_metadata_write PAUSE"

$CLICKHOUSE_CLIENT --query-id="$MUTATION_QUERY_ID" --query "ALTER TABLE $TABLE UPDATE value = 'b' WHERE id = 1 SETTINGS mutations_sync = 1, lock_acquire_timeout = 120" &
MUTATION_PID=$!

for _ in {1..600}
do
    if [[ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$MUTATION_QUERY_ID'")" -gt 0 ]]
    then
        break
    fi
    sleep 0.1
done

# Give the mutation a chance to misbehave: with the ALTER still parked it must not have published an entry.
sleep 1
$CLICKHOUSE_CLIENT --query "SELECT 'mutations published while the ALTER is parked', count() FROM system.zookeeper WHERE path = '$ZK_PATH/mutations'"

# Resume the ALTER: it hits the injected failure and rolls the setting back.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT alter_settings_pause_before_metadata_write"
wait "$ALTER_PID" || true
grep -oF 'FAULT_INJECTED' "$ALTER_OUTPUT" | head -n 1

wait "$MUTATION_PID"

# The mutation was created only after the rollback, so it records no author and keeps the old entry format.
$CLICKHOUSE_CLIENT --query "SELECT 'author is empty', author = '' FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"
$CLICKHOUSE_CLIENT --query "SELECT 'entry format', substring(value, 1, 17) FROM system.zookeeper WHERE path = '$ZK_PATH/mutations'"
$CLICKHOUSE_CLIENT --query "SELECT 'mutated value', value FROM $TABLE"
$CLICKHOUSE_CLIENT --query "SELECT 'setting persisted', engine_full LIKE '%persist_mutation_author%' FROM system.tables WHERE database = currentDatabase() AND name = '$TABLE'"
