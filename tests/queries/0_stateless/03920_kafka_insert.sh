#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test uses a single-partition topic, and multiple replicas compete for partition assignment

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_TOPIC=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')
KAFKA_BROKER="127.0.0.1:9092"

# Create topic
rpk topic create $KAFKA_TOPIC -p 1 --brokers $KAFKA_BROKER > /dev/null 2>&1 && echo "Created topic."

# Create a Kafka table for producing (INSERT)
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_producer (key UInt64, value String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_producer_group',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100;
"

# Insert data via ClickHouse (ClickHouse as Kafka producer)
# --send_logs_level=error: suppress librdkafka's "sasl.kerberos.kinit.cmd configuration parameter is ignored" warning
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    INSERT INTO ${CLICKHOUSE_TEST_UNIQUE_NAME}_producer VALUES (1, 'hello'), (2, 'world'), (3, 'test');
"

# Create a Kafka table for consuming (SELECT)
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_consumer (key UInt64, value String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_consumer_group',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100;
"

# Create destination table and materialized view
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst (key UInt64, value String)
    ENGINE = MergeTree ORDER BY key;
"

$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_consumer;
"

# Wait for messages to be consumed (120s to allow for slow consumer group assignment)
for i in $(seq 1 120); do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst")
    if [ "$count" -ge 3 ]; then
        break
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "SELECT key, value FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst ORDER BY key"

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mv" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_dst" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_consumer" 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_producer" 2>/dev/null
timeout 10 rpk topic delete $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1
