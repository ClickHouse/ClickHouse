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

# Create topic
rpk topic create $KAFKA_TOPIC -p 1 --brokers $KAFKA_BROKER > /dev/null 2>&1 && echo "Created topic."

# Produce messages with keys
for i in $(seq 1 3); do
    echo "key_$i:{\"id\": $i, \"data\": \"row_$i\"}"
done | timeout 30 rpk topic produce $KAFKA_TOPIC --brokers $KAFKA_BROKER -f '%k:%v\n' > /dev/null 2>&1

# Create Kafka engine table
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka (id UInt64, data String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '$KAFKA_GROUP',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100;
"

# Create destination table with virtual columns
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst (
        id UInt64,
        data String,
        kafka_topic String,
        kafka_key String,
        kafka_offset UInt64,
        kafka_partition UInt64
    )
    ENGINE = MergeTree ORDER BY id;
"

# Create materialized view capturing virtual columns
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst AS
    SELECT id, data,
           _topic AS kafka_topic,
           _key AS kafka_key,
           _offset AS kafka_offset,
           _partition AS kafka_partition
    FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka;
"

# Wait for messages to be consumed
for i in $(seq 1 30); do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst")
    if [ "$count" -ge 3 ]; then
        break
    fi
    sleep 1
done

# Verify virtual columns are populated
$CLICKHOUSE_CLIENT -q "SELECT id, data, kafka_key, kafka_offset, kafka_partition FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst ORDER BY id"

# Verify topic name
$CLICKHOUSE_CLIENT -q "SELECT kafka_topic = '$KAFKA_TOPIC' AS topic_matches FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst LIMIT 1"

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka" 2>/dev/null
timeout 10 rpk topic delete $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1
