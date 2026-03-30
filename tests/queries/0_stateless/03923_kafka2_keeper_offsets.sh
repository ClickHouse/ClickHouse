#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test uses a single-partition topic, and multiple replicas compete for partition assignment

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_TOPIC=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')
KAFKA_GROUP="${CLICKHOUSE_TEST_UNIQUE_NAME}_group"
KAFKA_BROKER="127.0.0.1:9092"
KEEPER_PATH="/clickhouse/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Create topic
rpk topic create $KAFKA_TOPIC -p 1 --brokers $KAFKA_BROKER > /dev/null 2>&1 && echo "Created topic."

# Produce first batch
for i in $(seq 1 3); do
    echo "{\"id\": $i, \"data\": \"batch1_$i\"}"
done | timeout 30 rpk topic produce $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1

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

# Produce second batch
for i in $(seq 4 6); do
    echo "{\"id\": $i, \"data\": \"batch2_$i\"}"
done | timeout 30 rpk topic produce $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1

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

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka" 2>/dev/null
timeout 10 rpk topic delete $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1
