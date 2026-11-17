#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Kafka and the Keeper client are not available in fast tests
# Tag no-replicated-database: the test drives activation of a single fixed replica identity

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_TOPIC=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')
KAFKA_BROKER="127.0.0.1:9092"
KEEPER_PATH="/clickhouse/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"
REPLICA_DIR="$KEEPER_PATH/replicas/migrated"
FOREIGN_SESSION_PID=""
FOREIGN_FIFO=""

POLL_TIMEOUT_MS=50

cleanup()
{
    local exit_code=$?

    trap - EXIT INT TERM
    set +e

    exec 3>&- 2>/dev/null
    [ -n "$FOREIGN_SESSION_PID" ] && kill "$FOREIGN_SESSION_PID" 2>/dev/null
    [ -n "$FOREIGN_FIFO" ] && rm -f "$FOREIGN_FIFO"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_migrated" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_anchor" 2>/dev/null
    timeout 10 rpk topic delete $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1

    exit $exit_code
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Create the topic. "Already exists" counts as success.
created=0
deadline=$((SECONDS + 30))
while [ "$SECONDS" -lt "$deadline" ]; do
    create_out=$(timeout 10 rpk topic create $KAFKA_TOPIC -p 1 --brokers $KAFKA_BROKER 2>&1)
    if [ $? -eq 0 ] || echo "$create_out" | grep -q "TOPIC_ALREADY_EXISTS"; then
        created=1
        break
    fi
    sleep 1
done
if [ "$created" -ne 1 ]; then
    echo "Failed to create Kafka topic within budget. Last rpk output: $create_out" >&2
    exit 1
fi
echo "Created topic."

# The anchor table only creates the shared Keeper tree of the table.
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_anchor (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_anchor',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_consumer_reschedule_ms = 100,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'anchor';
 "

# Plant the migrated identity and its capability marker on a foreign live session, as if
# the identity had been active elsewhere and unregistered without releasing the marker
# (the marker must belong to the registration, not to whichever session planted it).
# The fifo keeps the client's stdin open for as long as that session has to stay alive.
FOREIGN_FIFO=$(mktemp -u /tmp/clickhouse_foreign_session.XXXXXX)
mkfifo "$FOREIGN_FIFO"
$CLICKHOUSE_KEEPER_CLIENT --tests-mode < "$FOREIGN_FIFO" > /dev/null 2>&1 &
FOREIGN_SESSION_PID=$!
exec 3<>"$FOREIGN_FIFO"
printf '%s\n' "touch '$REPLICA_DIR'" "create '$REPLICA_DIR/demand_aware' 'stale' EPHEMERAL" >&3

marker_count() {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$REPLICA_DIR' AND name = 'demand_aware'" 2>/dev/null || echo 0
}

node_owner() {
    $CLICKHOUSE_CLIENT -q "SELECT ephemeralOwner FROM system.zookeeper WHERE path = '$REPLICA_DIR' AND name = '$1'" 2>/dev/null
}

planted=0
deadline=$((SECONDS + 60))
while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(marker_count)" -ge 1 ]; then
        planted=1
        break
    fi
    sleep 1
done
if [ "$planted" -ne 1 ]; then
    echo "FAIL: the foreign session did not plant the stale marker" >&2
    exit 1
fi
echo "--- Stale marker planted on a foreign live session ---"

# A successful activation of the identity must own the marker on its own session.
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_migrated (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_group',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_consumer_reschedule_ms = 100,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'migrated';
 "

activated=0
deadline=$((SECONDS + 60))
while [ "$SECONDS" -lt "$deadline" ]; do
    if $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$REPLICA_DIR' AND name = 'is_active'" 2>/dev/null | grep -q 1; then
        activated=1
        break
    fi
    sleep 1
done
if [ "$activated" -ne 1 ]; then
    echo "FAIL: the migrated replica did not activate" >&2
    exit 1
fi

# The activation creates `is_active` and takes the marker in one run, but the test may
# observe the intermediate moment, so the ownership check is polled, not one-shot.
owners_agree=0
deadline=$((SECONDS + 30))
while [ "$SECONDS" -lt "$deadline" ]; do
    marker_owner=$(node_owner demand_aware)
    active_owner=$(node_owner is_active)
    if [ -n "$marker_owner" ] && [ -n "$active_owner" ] && [ "$marker_owner" = "$active_owner" ]; then
        owners_agree=1
        break
    fi
    sleep 1
done
if [ "$owners_agree" -ne 1 ]; then
    echo "FAIL: activation did not take ownership of the marker (owner $marker_owner, is_active owner $active_owner)" >&2
    exit 1
fi
echo "--- Activation took ownership of the marker ---"

# The expiry of the original session must not remove the marker of an active replica.
exec 3>&-
kill "$FOREIGN_SESSION_PID" 2>/dev/null
wait "$FOREIGN_SESSION_PID" 2>/dev/null
FOREIGN_SESSION_PID=""
rm -f "$FOREIGN_FIFO"
FOREIGN_FIFO=""
sleep 7
if [ "$(marker_count)" -lt 1 ]; then
    echo "FAIL: the marker disappeared together with the foreign session although the replica is active" >&2
    exit 1
fi
echo "--- Marker survived the expiry of the foreign session ---"
