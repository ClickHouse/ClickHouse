#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-llvm-coverage
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test relies on exactly one real replica plus a ghost replica in Keeper competing for partition assignment
# Tag no-llvm-coverage: Kafka consumer is too slow under coverage instrumentation, consumer group rebalancing times out

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_TOPIC=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')
KAFKA_GROUP="${CLICKHOUSE_TEST_UNIQUE_NAME}_group"
KAFKA_BROKER="127.0.0.1:9092"
KEEPER_PATH="/clickhouse/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"

PARTITIONS=4
MESSAGES_PER_PARTITION=2
# Refresh rounds are counted in polls (stream_kafka_locks_refresh_polls=15);
# a short poll timeout keeps a round around ~15s so the test stays within budget.
POLL_TIMEOUT_MS=1000

cleanup()
{
    local exit_code=$?

    trap - EXIT INT TERM
    set +e

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka" 2>/dev/null
    timeout 10 rpk topic delete $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1

    exit $exit_code
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# End offset (high-watermark) of one partition. Prints the offset on success,
# fails closed (non-zero) when it cannot be read so the caller retries instead
# of re-producing committed messages.
partition_end_offset() {
    local partition=$1 out v
    out=$(timeout 10 rpk topic describe "$KAFKA_TOPIC" -p --brokers "$KAFKA_BROKER" 2>&1)
    v=$(echo "$out" | awk -v p="$partition" '$1==p {print $NF}')
    if [[ "$v" =~ ^[0-9]+$ ]]; then
        echo "$v"
        return 0
    fi
    echo "$out"
    return 1
}

# Produce $MESSAGES_PER_PARTITION messages into one partition, idempotently:
# message k (0-based) of partition p carries id p+1+4*k, so every partition
# receives a distinct, restartable id sequence. The deadline is shared by all
# partitions so the whole produce phase stays bounded well below the per-test
# timeout (see the budget comment in lock_count wait below).
produce_into_partition() {
    local partition=$1 deadline=$2
    local ids=()
    local k
    for k in $(seq 0 $((MESSAGES_PER_PARTITION - 1))); do
        ids+=($((partition + 1 + PARTITIONS * k)))
    done

    local hw out start
    while [ "$SECONDS" -lt "$deadline" ]; do
        if ! hw=$(partition_end_offset "$partition"); then
            sleep 1
            continue
        fi
        if [ "$hw" -ge "$MESSAGES_PER_PARTITION" ]; then
            return 0
        fi
        start=$hw
        out=$(seq "$start" $((MESSAGES_PER_PARTITION - 1)) | while read -r k; do
                echo "{\"id\": ${ids[$k]}, \"data\": \"msg_${ids[$k]}\"}"
            done | timeout 30 rpk topic produce "$KAFKA_TOPIC" -p "$partition" --brokers "$KAFKA_BROKER" 2>&1)
        sleep 1
    done
    echo "Failed to produce into partition $partition within budget. Last rpk output: ${out:-$hw}" >&2
    exit 1
}

# Create the topic. "Already exists" counts as success.
created=0
deadline=$((SECONDS + 60))
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

# Create Kafka2 engine table; this also creates the Keeper tree under KEEPER_PATH
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '$KAFKA_GROUP',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = '{replica}';
 "

# Register a ghost replica: present under replicas/ and "active" (is_active
# exists), but it never takes any partition lock -- e.g. a replica stuck in a
# metadata-retry loop. While such a replica exists, the temporary-lock quota
# logic used to cap this real replica's coverage at permanent quota + 1
# (4 / 2 = 2 permanent locks plus at most one rotating temporary lock), so the
# topic was never fully covered.
ghost_registered=0
deadline=$((SECONDS + 30))
while [ "$SECONDS" -lt "$deadline" ]; do
    if $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas', 'ghost', '')" 2>/dev/null \
        && $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$KEEPER_PATH/replicas/ghost', 'is_active', 'ghost')" 2>/dev/null; then
        ghost_registered=1
        break
    fi
    sleep 1
done
if [ "$ghost_registered" -ne 1 ]; then
    echo "Failed to register the ghost replica in Keeper" >&2
    exit 1
fi

# Destination table and the materialized view start the consumer threads
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

# The single real replica must be able to cover the whole topic: 2 permanent
# locks plus temporary locks for the remaining partitions. While the ghost is
# active this must still reach $PARTITIONS and stay there. All wait budgets
# (210s here + 90s produce + 90s consumption + startup loops) together stay
# well below the runner's 600s per-test timeout, so failures are always
# explicit messages, never a silent timeout.
locks=0
deadline=$((SECONDS + 210))
while [ "$SECONDS" -lt "$deadline" ]; do
    locks=$(lock_count)
    if [ "$locks" -ge "$PARTITIONS" ]; then
        break
    fi
    sleep 2
done
if [ "$locks" -lt "$PARTITIONS" ]; then
    echo "FAIL: expected $PARTITIONS topic partition locks with the ghost replica active, got $locks (temporary-lock quota is capped while a lockless replica exists)" >&2
    exit 1
fi

# Produce two messages into every partition: with full lock coverage all of
# them must be consumed. One shared produce budget covers all partitions.
produce_deadline=$((SECONDS + 90))
for p in $(seq 0 $((PARTITIONS - 1))); do
    produce_into_partition $p $produce_deadline
done

expected=$((PARTITIONS * MESSAGES_PER_PARTITION))
count=0
deadline=$((SECONDS + 90))
while [ "$SECONDS" -lt "$deadline" ]; do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst SETTINGS max_execution_time=5" 2>/dev/null || echo 0)
    if [ "$count" -ge "$expected" ]; then
        break
    fi
    sleep 1
done
if [ "$count" -lt "$expected" ]; then
    echo "FAIL: expected $expected consumed messages with the ghost replica active, got $count" >&2
    exit 1
fi

echo "--- Consumed rows ---"
$CLICKHOUSE_CLIENT -q "SELECT id, data FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst ORDER BY id"
