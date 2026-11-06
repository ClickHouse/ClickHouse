#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-llvm-coverage
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test uses a single-partition topic, and multiple replicas compete for partition assignment
# Tag no-llvm-coverage: Kafka consumer is too slow under coverage instrumentation, consumer group rebalancing times out

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_TOPIC=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')
KAFKA_GROUP="${CLICKHOUSE_TEST_UNIQUE_NAME}_group"
KAFKA_BROKER="127.0.0.1:9092"
KEEPER_PATH="/clickhouse/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"

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

# On a loaded runner the broker can be briefly unavailable, so every broker
# operation is retried until it succeeds or a wall-clock budget is exhausted.
# Budgets stay well below the clickhouse-test per-test timeout so the explicit
# failure path always runs instead of the test silently timing out.

# End offset (high-watermark) of partition 0, i.e. the number of committed
# records. On success prints the offset and returns 0. When the offset cannot be
# read (topic missing, broker unreachable, metadata not yet propagated) it fails
# closed: prints the rpk output and returns non-zero so the caller retries the
# read instead of assuming offset 0 (which would re-produce committed ids).
topic_end_offset() {
    local out v
    out=$(timeout 10 rpk topic describe "$KAFKA_TOPIC" -p --brokers "$KAFKA_BROKER" 2>&1)
    v=$(echo "$out" | awk '$1=="0" {print $NF}')
    if [[ "$v" =~ ^[0-9]+$ ]]; then
        echo "$v"
        return 0
    fi
    echo "$out"
    return 1
}

# Produce ids [first..last] into the single-partition topic, idempotently.
# Each id is written in order, so id N lands at offset N-1 and the end offset
# equals the count of committed ids. We only ever send ids that are not yet
# committed (suffix starting at end_offset+1), so a retry after a partial
# produce completes the batch instead of re-sending acknowledged records.
# Success is decided by the end offset, not by rpk's exit code.
produce_ids() {
    local first=$1 last=$2 prefix=$3
    local deadline=$((SECONDS + 60))
    local out hw start last_err=""
    while [ "$SECONDS" -lt "$deadline" ]; do
        # Fail closed on an unreadable end offset: retry the read without
        # producing. Treating it as 0 would re-send already-committed ids.
        if ! hw=$(topic_end_offset); then
            last_err=$hw
            sleep 1
            continue
        fi
        if [ "$hw" -ge "$last" ]; then
            return 0
        fi
        start=$((hw + 1))
        [ "$start" -lt "$first" ] && start=$first
        out=$(seq "$start" "$last" | while read -r i; do echo "{\"id\": $i, \"data\": \"${prefix}_$i\"}"; done \
            | timeout 30 rpk topic produce "$KAFKA_TOPIC" --brokers "$KAFKA_BROKER" 2>&1)
        sleep 1
    done
    echo "Failed to produce ids [$first..$last] within budget. Last rpk output: ${out:-$last_err}" >&2
    exit 1
}

# Create the topic. Treat "already exists" as success, so the loop is
# idempotent; on exhausted budget fail loudly with the last rpk output.
created=0
deadline=$((SECONDS + 60))
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

# Produce first batch (ids 1..3)
produce_ids 1 3 batch1

# Create Kafka2 engine table (with keeper path for offset storage)
$CLICKHOUSE_CLIENT --allow_experimental_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '$KAFKA_GROUP',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100,
             kafka_keeper_path = '$KEEPER_PATH',
             kafka_replica_name = 'r1';
"

# Create destination table
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst (id UInt64, data String)
    ENGINE = MergeTree ORDER BY id;
"

# Create materialized view
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka;
"

# Wait for first batch (Kafka2 with keeper path needs extra startup time for Keeper coordination)
for i in $(seq 1 120); do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst SETTINGS max_execution_time=5" 2>/dev/null || echo 0)
    if [ "$count" -ge 3 ]; then
        break
    fi
    sleep 1
done

echo "--- After first batch ---"
$CLICKHOUSE_CLIENT -q "SELECT id, data FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst ORDER BY id"

# Produce second batch (ids 4..6)
produce_ids 4 6 batch2

# Wait for second batch
for i in $(seq 1 120); do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst SETTINGS max_execution_time=5" 2>/dev/null || echo 0)
    if [ "$count" -ge 6 ]; then
        break
    fi
    sleep 1
done

echo "--- After second batch ---"
$CLICKHOUSE_CLIENT -q "SELECT id, data FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst ORDER BY id"
