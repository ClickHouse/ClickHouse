#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the test uses a single-partition topic, and multiple replicas compete for partition assignment

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KAFKA_BROKER="127.0.0.1:9092"
KAFKA_PRODUCER_OPTS="--producer-property delivery.timeout.ms=30000 --producer-property linger.ms=0"
KAFKA_BASE=$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr '_' '-')

# Test JSONEachRow format
KAFKA_TOPIC="${KAFKA_BASE}-json"
timeout 30 kafka-topics.sh --bootstrap-server $KAFKA_BROKER --create --topic $KAFKA_TOPIC \
    --partitions 1 --replication-factor 1 2>/dev/null | sed 's/Created topic .*/Created topic./'

for i in $(seq 1 3); do
    echo "{\"a\": $i, \"b\": \"json_$i\"}"
done | timeout 30 kafka-console-producer.sh --bootstrap-server $KAFKA_BROKER --topic $KAFKA_TOPIC \
    $KAFKA_PRODUCER_OPTS 2>/dev/null

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
timeout 30 kafka-topics.sh --bootstrap-server $KAFKA_BROKER --create --topic $KAFKA_TOPIC_CSV \
    --partitions 1 --replication-factor 1 2>/dev/null | sed 's/Created topic .*/Created topic./'

for i in $(seq 1 3); do
    echo "$i,\"csv_$i\""
done | timeout 30 kafka-console-producer.sh --bootstrap-server $KAFKA_BROKER --topic $KAFKA_TOPIC_CSV \
    $KAFKA_PRODUCER_OPTS 2>/dev/null

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
timeout 30 kafka-topics.sh --bootstrap-server $KAFKA_BROKER --create --topic $KAFKA_TOPIC_TSV \
    --partitions 1 --replication-factor 1 2>/dev/null | sed 's/Created topic .*/Created topic./'

for i in $(seq 1 3); do
    printf '%d\ttsv_%d\n' "$i" "$i"
done | timeout 30 kafka-console-producer.sh --bootstrap-server $KAFKA_BROKER --topic $KAFKA_TOPIC_TSV \
    $KAFKA_PRODUCER_OPTS 2>/dev/null

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

# Wait for all messages to be consumed
for i in $(seq 1 30); do
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
timeout 10 kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic $KAFKA_TOPIC 2>/dev/null
timeout 10 kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic $KAFKA_TOPIC_CSV 2>/dev/null
timeout 10 kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic $KAFKA_TOPIC_TSV 2>/dev/null
