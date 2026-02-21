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

# Produce messages
for i in $(seq 1 5); do
    echo "{\"key\": $i, \"value\": \"message_$i\"}"
done | timeout 30 rpk topic produce $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1

# Create Kafka engine table
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka (key UInt64, value String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '$KAFKA_GROUP',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100;
"

# Create destination table
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst (key UInt64, value String)
    ENGINE = MergeTree ORDER BY key;
"

# Create materialized view
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka;
"

# Wait for messages to be consumed
for i in $(seq 1 30); do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst")
    if [ "$count" -ge 5 ]; then
        break
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "SELECT key, value FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst ORDER BY key"

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka" 2>/dev/null
timeout 10 rpk topic delete $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1
