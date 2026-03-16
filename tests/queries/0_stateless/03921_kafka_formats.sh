#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test uses a single-partition topic, and multiple replicas compete for partition assignment

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_BROKER="127.0.0.1:9092"
KAFKA_BASE=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')

# Test JSONEachRow format
KAFKA_TOPIC="${KAFKA_BASE}-json"
rpk topic create $KAFKA_TOPIC -p 1 --brokers $KAFKA_BROKER > /dev/null 2>&1 && echo "Created topic."

for i in $(seq 1 3); do
    echo "{\"a\": $i, \"b\": \"json_$i\"}"
done | timeout 30 rpk topic produce $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_json_kafka (a UInt64, b String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_json_group',
             kafka_format = 'JSONEachRow',
             kafka_max_block_size = 100;
"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_json_dst (a UInt64, b String)
    ENGINE = MergeTree ORDER BY a;
"
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_json_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_json_dst AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_json_kafka;
"

# Test CSV format
KAFKA_TOPIC_CSV="${KAFKA_BASE}-csv"
rpk topic create $KAFKA_TOPIC_CSV -p 1 --brokers $KAFKA_BROKER > /dev/null 2>&1 && echo "Created topic."

for i in $(seq 1 3); do
    echo "$i,\"csv_$i\""
done | timeout 30 rpk topic produce $KAFKA_TOPIC_CSV --brokers $KAFKA_BROKER > /dev/null 2>&1

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_csv_kafka (a UInt64, b String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC_CSV',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_csv_group',
             kafka_format = 'CSV',
             kafka_max_block_size = 100;
"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_csv_dst (a UInt64, b String)
    ENGINE = MergeTree ORDER BY a;
"
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_csv_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_csv_dst AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_csv_kafka;
"

# Test TSV format
KAFKA_TOPIC_TSV="${KAFKA_BASE}-tsv"
rpk topic create $KAFKA_TOPIC_TSV -p 1 --brokers $KAFKA_BROKER > /dev/null 2>&1 && echo "Created topic."

for i in $(seq 1 3); do
    printf '%d\ttsv_%d\n' "$i" "$i"
done | timeout 30 rpk topic produce $KAFKA_TOPIC_TSV --brokers $KAFKA_BROKER > /dev/null 2>&1

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_tsv_kafka (a UInt64, b String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '$KAFKA_BROKER',
             kafka_topic_list = '$KAFKA_TOPIC_TSV',
             kafka_group_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_tsv_group',
             kafka_format = 'TSV',
             kafka_max_block_size = 100;
"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_tsv_dst (a UInt64, b String)
    ENGINE = MergeTree ORDER BY a;
"
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW ${CLICKHOUSE_TEST_UNIQUE_NAME}_tsv_mv TO ${CLICKHOUSE_TEST_UNIQUE_NAME}_tsv_dst AS
    SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_tsv_kafka;
"

# Wait for all messages to be consumed (120s to allow for slow consumer group assignment)
for i in $(seq 1 120); do
    json_count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_json_dst")
    csv_count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_csv_dst")
    tsv_count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_tsv_dst")
    if [ "$json_count" -ge 3 ] && [ "$csv_count" -ge 3 ] && [ "$tsv_count" -ge 3 ]; then
        break
    fi
    sleep 1
done

echo "--- JSONEachRow ---"
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_json_dst ORDER BY a"

echo "--- CSV ---"
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_csv_dst ORDER BY a"

echo "--- TSV ---"
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_tsv_dst ORDER BY a"

# Cleanup
for fmt in json csv tsv; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_${fmt}_mv" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_${fmt}_dst" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_${fmt}_kafka" 2>/dev/null
done
timeout 10 rpk topic delete $KAFKA_TOPIC --brokers $KAFKA_BROKER > /dev/null 2>&1
timeout 10 rpk topic delete $KAFKA_TOPIC_CSV --brokers $KAFKA_BROKER > /dev/null 2>&1
timeout 10 rpk topic delete $KAFKA_TOPIC_TSV --brokers $KAFKA_BROKER > /dev/null 2>&1
