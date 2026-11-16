#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-llvm-coverage, long
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test relies on a fixed set of replicas (a healthy one, a permanently lockless ghost, a stuck and a fresh one) competing for partition assignment
# Tag no-llvm-coverage: Kafka consumer is too slow under coverage instrumentation, consumer group rebalancing times out
# Tag long: lock refresh rounds are wall-clock bound (~15 polls per round), so even the green path takes minutes on slow builders

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_TOPIC=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')
KAFKA_GROUP="${CLICKHOUSE_TEST_UNIQUE_NAME}_group"
KAFKA_BROKER="127.0.0.1:9092"
KEEPER_PATH="/clickhouse/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"

PARTITIONS=4
MESSAGES_PER_PARTITION=2
MAX_MESSAGES_PER_PARTITION=8
# Refresh rounds are counted in polls (stream_kafka_locks_refresh_polls=15);
# short poll + fast reschedule keep a refresh round at a few seconds (the combination the integration quota tests use).
POLL_TIMEOUT_MS=50

cleanup()
{
    local exit_code=$?

    trap - EXIT INT TERM
    set +e

    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv_b" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv_broken" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_b" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_b" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_broken" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv_r4" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_r4" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_r4" 2>/dev/null
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

# Produce messages into one partition, idempotently, until it holds $3 records:
# message k (0-based) of partition p carries id p+1+4*k. The deadline is shared by all
# partitions so the whole produce phase stays bounded well below the per-test timeout.
produce_into_partition() {
    local partition=$1 deadline=$2 target=$3
    local ids=()
    local k
    for k in $(seq 0 $((MAX_MESSAGES_PER_PARTITION - 1))); do
        ids+=($((partition + 1 + PARTITIONS * k)))
    done

    local hw out start
    while [ "$SECONDS" -lt "$deadline" ]; do
        if ! hw=$(partition_end_offset "$partition"); then
            sleep 1
            continue
        fi
        if [ "$hw" -ge "$target" ]; then
            return 0
        fi
        start=$hw
        out=$(seq "$start" $((target - 1)) | while read -r k; do
                echo "{\"id\": ${ids[$k]}, \"data\": \"msg_${ids[$k]}\"}"
            done | timeout 30 rpk topic produce "$KAFKA_TOPIC" -p "$partition" --brokers "$KAFKA_BROKER" 2>&1)
        sleep 1
    done
    echo "Failed to produce into partition $partition within budget. Last rpk output: ${out:-$hw}" >&2
    exit 1
}

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
             kafka_consumer_reschedule_ms = 100,
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

# A fourth replica is created now but starts consuming only in the last act: its
# is_active exists (the activating task creates it without a view), so by then it
# is the same long-lockless identity recovering, not a fresh replica.
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_r4 (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${KAFKA_GROUP}_r4',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_consumer_reschedule_ms = 1000,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'r4';
 "
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_r4 (id UInt64, data String)
    ENGINE = MergeTree ORDER BY id;
 "

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

# The single real replica must be able to cover the whole topic: 2 permanent locks plus
# temporary locks for the remaining partitions. The ghost is lockless forever, so after
# MAX_ROUNDS_TO_YIELD_TO_LOCKLESS_REPLICA rounds the real replica stops yielding to it
# and the coverage must reach $PARTITIONS. All wait budgets together stay well below the
# runner's per-test timeout, so failures are always explicit messages, never a silent
# timeout.
locks=0
deadline=$((SECONDS + 95))
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

# Produce two messages into every partition: with full lock coverage all of them must be
# consumed. One shared produce budget covers all partitions.
produce_deadline=$((SECONDS + 40))
for p in $(seq 0 $((PARTITIONS - 1))); do
    produce_into_partition $p $produce_deadline $MESSAGES_PER_PARTITION
done

expected=$((PARTITIONS * MESSAGES_PER_PARTITION))
count=0
deadline=$((SECONDS + 50))
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

echo "--- Act 1 done: full coverage with a permanently lockless replica ---"

# Act 2: a replica that is active but cannot reach Kafka (metadata retry loop) holds no
# locks for a long time; it must not stop a fresh replica from taking its share. The
# fresh replica plays the role of one recovering after a restart.
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_broken (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '127.0.0.1:9',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${KAFKA_GROUP}_broken',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_consumer_reschedule_ms = 100,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'r2';
 "
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_b (id UInt64, data String)
    ENGINE = MergeTree ORDER BY id;
 "
# The MV makes r2 active: it holds no locks (NoMetadata loop) for the rest of the test,
# exactly like a replica stuck while its session stays alive.
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv_broken TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_b AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_broken;
 "

# Let the stuck replica's lockless streak age past the yield window.
sleep 25

# A fresh healthy replica starts: it is lockless and young, so the healthy first replica
# must yield the free partitions to it even though the stuck r2 (and the ghost) exist.
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_b (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${KAFKA_GROUP}_b',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_poll_timeout_ms = $POLL_TIMEOUT_MS,
             kafka_consumer_reschedule_ms = 100,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'r3';
 "
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv_b TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_b AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_b;
 "

recovered=0
deadline=$((SECONDS + 50))
while [ "$SECONDS" -lt "$deadline" ]; do
    recovered=$($CLICKHOUSE_CLIENT -q "SELECT length(assignments.partition_id) FROM system.kafka_consumers WHERE table = '${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_b' SETTINGS max_execution_time=5" 2>/dev/null || echo 0)
    if [ "$recovered" -ge 1 ]; then
        break
    fi
    sleep 2
done
if [ "$recovered" -lt 1 ]; then
    echo "FAIL: the fresh replica did not acquire any partition within the budget (free partitions are not yielded to it while older replicas are stuck lockless)" >&2
    exit 1
fi

# Produce one more message per partition: the fresh replica must consume its share
# (an intermediate target; the final act produces more).
produce_deadline=$((SECONDS + 30))
for p in $(seq 0 $((PARTITIONS - 1))); do
    produce_into_partition $p $produce_deadline $((MAX_MESSAGES_PER_PARTITION - 2))
done

count_b=0
deadline=$((SECONDS + 45))
while [ "$SECONDS" -lt "$deadline" ]; do
    count_b=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_b SETTINGS max_execution_time=5" 2>/dev/null || echo 0)
    if [ "$count_b" -ge 1 ]; then
        break
    fi
    sleep 2
done
if [ "$count_b" -lt 1 ]; then
    echo "FAIL: the fresh replica acquired partitions but consumed nothing" >&2
    exit 1
fi

echo "--- Act 2 done: the fresh replica acquired partitions and consumes despite a stuck lockless replica ---"

# Act 3: the same long-lockless replica r4 finally starts consuming (view attached).
# Nothing is free, so its first attempts fail and publish demand; the others must then
# reserve free partitions for it until it acquires them.
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv_r4 TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_r4 AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_r4;
 "

recovered4=0
deadline=$((SECONDS + 30))
while [ "$SECONDS" -lt "$deadline" ]; do
    recovered4=$($CLICKHOUSE_CLIENT -q "SELECT length(assignments.partition_id) FROM system.kafka_consumers WHERE table = '${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_r4' SETTINGS max_execution_time=5" 2>/dev/null || echo 0)
    if [ "$recovered4" -ge 1 ]; then
        break
    fi
    sleep 2
done
if [ "$recovered4" -lt 1 ]; then
    echo "FAIL: the same aged lockless replica did not acquire partitions after it started consuming (no demand handoff)" >&2
    exit 1
fi

produce_deadline=$((SECONDS + 30))
for p in $(seq 0 $((PARTITIONS - 1))); do
    produce_into_partition $p $produce_deadline $MAX_MESSAGES_PER_PARTITION
done

count_r4=0
deadline=$((SECONDS + 25))
while [ "$SECONDS" -lt "$deadline" ]; do
    count_r4=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst_r4 SETTINGS max_execution_time=5" 2>/dev/null || echo 0)
    if [ "$count_r4" -ge 1 ]; then
        break
    fi
    sleep 2
done
if [ "$count_r4" -lt 1 ]; then
    echo "FAIL: the recovered aged replica acquired partitions but consumed nothing" >&2
    exit 1
fi

# The demand must be withdrawn once the replica holds partitions again.
demand_gone=0
deadline=$((SECONDS + 10))
while [ "$SECONDS" -lt "$deadline" ]; do
    demand_gone=$($CLICKHOUSE_CLIENT -q "SELECT countIf(name = 'demand') FROM system.zookeeper WHERE path = '$KEEPER_PATH/replicas/r4'" 2>/dev/null || echo 1)
    if [ "$demand_gone" -eq 0 ]; then
        break
    fi
    sleep 1
done
if [ "$demand_gone" -ne 0 ]; then
    echo "FAIL: the recovered replica still keeps its demand node" >&2
    exit 1
fi

echo "--- Act 3 done: the same aged replica recovered via the demand handoff ---"

echo "--- Consumed rows ---"
$CLICKHOUSE_CLIENT -q "SELECT id, data FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst WHERE id <= $((PARTITIONS * MESSAGES_PER_PARTITION)) ORDER BY id"
