#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-llvm-coverage, long
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test relies on a fixed set of replicas (a healthy one, a ghost and a viewless one) competing for partition assignment
# Tag no-llvm-coverage: Kafka consumer is too slow under coverage instrumentation, consumer group rebalancing times out
# Tag long: lock refresh rounds are wall-clock bound (~15 polls per round), so even the green path takes minutes on slow builders

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
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_idle" 2>/dev/null
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

# A healthy consuming replica and a viewless one whose is_active exists (the activating
# task creates it without a view), plus a permanently lockless ghost.
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
             kafka_replica_name = '{replica}';
 "
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_idle (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_idle',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_consumer_reschedule_ms = 20000,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'idle';
 "
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst (id UInt64, data String)
    ENGINE = MergeTree ORDER BY id;
 "
ghost_registered=0
deadline=$((SECONDS + 15))
while [ "$SECONDS" -lt "$deadline" ]; do
    if $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas', 'ghost', '')" 2>/dev/null \
        && $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas/ghost', 'is_active', 'ghost')" 2>/dev/null \
        && $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas/ghost', 'demand_aware', '')" 2>/dev/null; then
        ghost_registered=1
        break
    fi
    sleep 1
done
if [ "$ghost_registered" -ne 1 ]; then
    echo "Failed to register the ghost replica in Keeper" >&2
    exit 1
fi
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka;
 "

lock_count() {
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$KEEPER_PATH/topic_partition_locks'" 2>/dev/null || echo 0
}

# The healthy replica must cover the whole topic before the stale-demand part starts.
locks=0
deadline=$((SECONDS + 150))
while [ "$SECONDS" -lt "$deadline" ]; do
    locks=$(lock_count)
    if [ "$locks" -ge "$PARTITIONS" ]; then
        break
    fi
    sleep 2
done
if [ "$locks" -lt "$PARTITIONS" ]; then
    echo "FAIL: expected $PARTITIONS topic partition locks, got $locks (temporary-lock quota is capped while a lockless replica exists)" >&2
    exit 1
fi
echo "--- Full coverage reached ---"

# Freeze the healthy replica with its locks held: SYSTEM PAUSE blocks further streaming
# cycles without touching the consumers, so no refresh releases temporary locks while the
# one-shot read below runs, and the read cannot catch a rotation window.
$CLICKHOUSE_CLIENT -q "SYSTEM PAUSE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka"
sleep 5

# A single direct SELECT on the viewless replica: its consumer tries to take partitions,
# finds none and publishes demand; the query finishes and nothing retries, while the
# replica's session and is_active stay alive.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_idle SETTINGS stream_like_engine_allow_direct_select = 1, max_execution_time = 30" > /dev/null

demand_seen=0
deadline=$((SECONDS + 10))
while [ "$SECONDS" -lt "$deadline" ]; do
    demand_seen=$($CLICKHOUSE_CLIENT -q "SELECT countIf(name = 'demand') FROM system.zookeeper WHERE path = '$KEEPER_PATH/replicas/idle'" 2>/dev/null || echo 0)
    if [ "$demand_seen" -ge 1 ]; then
        break
    fi
    sleep 1
done
if [ "$demand_seen" -lt 1 ]; then
    echo "FAIL: the viewless replica did not publish demand although nothing is free" >&2
    exit 1
fi
demand_time=$SECONDS
echo "--- Demand published by a replica that stopped trying ---"

# Resume the healthy replica: it must see the demand, hold a partition reserved for the
# demanding (but not retrying) replica, and lift the reservation once the demand retires.
$CLICKHOUSE_CLIENT -q "SYSTEM START ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka"

# The fresh demand must hold the reservation: the healthy replica leaves one free
# partition for the demanding (but not retrying) replica. Verify the deficit is
# sustained for most of the declared retention — a transient dip from temporary-lock
# rotation recovers within one refresh round, and a too-short retention recovers
# well before the declared interval elapses.
locks3=999
deadline=$((SECONDS + 30))
while [ "$SECONDS" -lt "$deadline" ]; do
    locks3=$(lock_count)
    if [ "$locks3" -le $((PARTITIONS - 1)) ]; then
        break
    fi
    sleep 2
done
if [ "$locks3" -gt $((PARTITIONS - 1)) ]; then
    echo "FAIL: the fresh demand did not hold the reservation (locks stayed at $locks3)" >&2
    exit 1
fi
sustain_deadline=$((demand_time + 50))
while [ "$SECONDS" -lt "$sustain_deadline" ]; do
    locks3=$(lock_count)
    if [ "$locks3" -gt $((PARTITIONS - 1)) ]; then
        echo "FAIL: the deficit recovered at $((SECONDS - demand_time))s — before the declared retention elapsed" >&2
        exit 1
    fi
    sleep 5
done
echo "--- Fresh demand holds the reservation ---"

# The demand value never changes again, so after its declared retention the
# reservation must lift and full coverage must return.
locks4=0
deadline=$((demand_time + 150))
while [ "$SECONDS" -lt "$deadline" ]; do
    locks4=$(lock_count)
    if [ "$locks4" -ge "$PARTITIONS" ]; then
        break
    fi
    sleep 2
done
if [ "$locks4" -lt "$PARTITIONS" ]; then
    echo "FAIL: a stale demand still holds the reservation (got $locks4 locks, expected $PARTITIONS)" >&2
    exit 1
fi
echo "--- Stale demand stopped holding the reservation: full coverage returned ---"
