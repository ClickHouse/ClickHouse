#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-llvm-coverage, long
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test relies on fixed replica identities (a real one, a reclaimed one and two ghosts) competing for partition assignment
# Tag no-llvm-coverage: Kafka consumer is too slow under coverage instrumentation, consumer group rebalancing times out
# Tag long: lock refresh rounds are wall-clock bound (~15 polls per round), so even the green path takes a minute on slow builders

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_TOPIC=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')
KAFKA_BROKER="127.0.0.1:9092"
KEEPER_PATH="/clickhouse/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"

PARTITIONS=4
POLL_TIMEOUT_MS=50

cleanup()
{
    local exit_code=$?

    trap - EXIT INT TERM
    set +e

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_reclaimed" 2>/dev/null
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
    create_out=$(timeout 10 rpk topic create $KAFKA_TOPIC -p $PARTITIONS --brokers $KAFKA_BROKER 2>&1)
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

$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_group',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_consumer_reschedule_ms = 100,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'real';
  "
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst (id UInt64, data String)
    ENGINE = MergeTree ORDER BY id;
  "
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka;
  "

lock_count() {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$KEEPER_PATH/topic_partition_locks'" 2>/dev/null || echo 0
}

# A registration of the current code advertises it in the data of its is_active node.
aware=0
deadline=$((SECONDS + 30))
while [ "$SECONDS" -lt "$deadline" ]; do
    aware=$($CLICKHOUSE_CLIENT -q "SELECT countIf(name = 'is_active' AND endsWith(value, ':demand_aware')) FROM system.zookeeper WHERE path = '$KEEPER_PATH/replicas/real'" 2>/dev/null || echo 0)
    if [ "$aware" -ge 1 ]; then
        break
    fi
    sleep 1
done
if [ "$aware" -lt 1 ]; then
    echo "FAIL: the active registration does not advertise demand awareness in its is_active node" >&2
    exit 1
fi
echo "--- The active registration advertises demand awareness ---"

# Alone, the real replica holds the whole topic.
locks=0
deadline=$((SECONDS + 60))
while [ "$SECONDS" -lt "$deadline" ]; do
    locks=$(lock_count)
    if [ "$locks" -ge "$PARTITIONS" ]; then
        break
    fi
    sleep 1
done
if [ "$locks" -lt "$PARTITIONS" ]; then
    echo "FAIL: expected $PARTITIONS locks while the replica is alone, got $locks" >&2
    exit 1
fi

# Two lockless ghosts: one of the current code, one of the old code (no suffix). Both are
# yielded to while young; after the window only the old-code one keeps its quota reserved.
ghosts_registered=0
deadline=$((SECONDS + 15))
while [ "$SECONDS" -lt "$deadline" ]; do
    if $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas', 'ghost', '')" 2>/dev/null \
        && $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas/ghost', 'is_active', 'ghost:demand_aware')" 2>/dev/null \
        && $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas', 'legacy', '')" 2>/dev/null \
        && $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas/legacy', 'is_active', 'legacy')" 2>/dev/null; then
        ghosts_registered=1
        break
    fi
    sleep 1
done
if [ "$ghosts_registered" -ne 1 ]; then
    echo "Failed to register the ghost replicas in Keeper" >&2
    exit 1
fi

# With three active replicas the quota is one partition each: one permanent lock plus one
# temporary lock for the real replica, one partition reserved for each ghost.
locks=999
deadline=$((SECONDS + 60))
while [ "$SECONDS" -lt "$deadline" ]; do
    locks=$(lock_count)
    if [ "$locks" -le 2 ]; then
        break
    fi
    sleep 1
done
if [ "$locks" -gt 2 ]; then
    echo "FAIL: the real replica did not yield to the young lockless replicas (got $locks locks)" >&2
    exit 1
fi

# Once the window of the current-code ghost lapses, its partition is covered again; the
# old-code ghost keeps its reservation, so the coverage settles at PARTITIONS - 1.
locks=0
deadline=$((SECONDS + 150))
while [ "$SECONDS" -lt "$deadline" ]; do
    locks=$(lock_count)
    if [ "$locks" -ge $((PARTITIONS - 1)) ]; then
        break
    fi
    sleep 1
done
if [ "$locks" -ne $((PARTITIONS - 1)) ]; then
    echo "FAIL: expected $((PARTITIONS - 1)) locks with a lockless replica of the old code, got $locks" >&2
    exit 1
fi
# A refresh briefly releases the temporary locks, so only an excess of locks counts.
hold_deadline=$((SECONDS + 20))
while [ "$SECONDS" -lt "$hold_deadline" ]; do
    sleep 2
    locks=$(lock_count)
    if [ "$locks" -gt $((PARTITIONS - 1)) ]; then
        echo "FAIL: the reservation of the old-code replica lapsed (got $locks locks)" >&2
        exit 1
    fi
done
echo "--- A lockless replica of the old code keeps its quota reserved ---"

# The previous version wrote is_active with the bare server UUID. A node like that must be
# reclaimed by the activation of the same replica: replaced by an ephemeral one with the suffix.
server_uuid=$($CLICKHOUSE_CLIENT -q "SELECT substring(value, 1, length(value) - length(':demand_aware')) FROM system.zookeeper WHERE path = '$KEEPER_PATH/replicas/real' AND name = 'is_active'" 2>/dev/null)
if [ -z "$server_uuid" ]; then
    echo "FAIL: cannot read the server UUID from the is_active node" >&2
    exit 1
fi
planted=0
if $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas', 'reclaimed', '')" 2>/dev/null \
    && $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas/reclaimed', 'is_active', '$server_uuid')" 2>/dev/null; then
    planted=1
fi
if [ "$planted" -ne 1 ]; then
    echo "FAIL: could not plant the is_active node of the previous version" >&2
    exit 1
fi
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_reclaimed (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_reclaimed_group',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_consumer_reschedule_ms = 100,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'reclaimed';
  "
reclaimed=0
deadline=$((SECONDS + 30))
while [ "$SECONDS" -lt "$deadline" ]; do
    reclaimed=$($CLICKHOUSE_CLIENT -q "SELECT countIf(ephemeralOwner != 0 AND endsWith(value, ':demand_aware')) FROM system.zookeeper WHERE path = '$KEEPER_PATH/replicas/reclaimed' AND name = 'is_active'" 2>/dev/null || echo 0)
    if [ "$reclaimed" -ge 1 ]; then
        break
    fi
    sleep 1
done
if [ "$reclaimed" -lt 1 ]; then
    echo "FAIL: activation did not reclaim the is_active node left by the previous version" >&2
    exit 1
fi
echo "--- Activation reclaims an is_active node of the previous version ---"
