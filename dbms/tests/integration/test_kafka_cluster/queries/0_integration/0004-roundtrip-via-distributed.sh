#!/usr/bin/env bash
DOCKER_COMPOSE=${DOCKER_COMPOSE:-docker-compose}

# just in case if some service is offline
$DOCKER_COMPOSE up -d --no-recreate >/dev/null 2>&1

# docker-compose exec clickhouse1 clickhouse client
# docker-compose exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list
# docker-compose exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --list
# docker-compose exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group dummytopic_consumer_group1
echo "1. clean old topic & consumer group"

$DOCKER_COMPOSE exec -T kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic dummytopic >/dev/null 2>&1
$DOCKER_COMPOSE exec -T kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group dummytopic_consumer_group1 >/dev/null 2>&1

echo "2. create topic"

$DOCKER_COMPOSE exec -T kafka1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 12 --topic dummytopic

echo "3. create src table"

cat <<HEREDOC | $DOCKER_COMPOSE exec -T clickhouse1 clickhouse client -n
DROP TABLE IF EXISTS source_table;
CREATE TABLE source_table ENGINE = Log AS SELECT toUInt32(number) as id from numbers(240000);
HEREDOC

echo "4. copy table data to topic"

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT * FROM source_table FORMAT JSONEachRow' | $DOCKER_COMPOSE exec -T kafka1 kafka-console-producer --broker-list kafka1:9092,kafka2:9092,kafka3:9092 --topic dummytopic >/dev/null

echo "5. create dest tables pipeline"

cat <<HEREDOC | $DOCKER_COMPOSE exec -T clickhouse1 clickhouse client -n >/dev/null
DROP TABLE IF EXISTS dummy_queue ON CLUSTER sharded_cluster_secure;
DROP TABLE IF EXISTS dummy_local ON CLUSTER sharded_cluster_secure;
DROP TABLE IF EXISTS dummy ON CLUSTER sharded_cluster_secure;
DROP TABLE IF EXISTS dummy_mv ON CLUSTER sharded_cluster_secure;

CREATE TABLE dummy_queue ON CLUSTER sharded_cluster_secure (
    id UInt32
) ENGINE = Kafka('kafka1:9092,kafka2:9092,kafka3:9092', 'dummytopic', 'dummytopic_consumer_group1', 'JSONEachRow');

CREATE TABLE dummy_local ON CLUSTER sharded_cluster_secure (
    id UInt32
) ENGINE = MergeTree() ORDER BY (id);

CREATE TABLE dummy ON CLUSTER sharded_cluster_secure (
    id UInt32
) ENGINE = Distributed(sharded_cluster_secure, currentDatabase(), dummy_local, id);

CREATE MATERIALIZED VIEW dummy_mv ON CLUSTER sharded_cluster_secure TO dummy AS SELECT id FROM dummy_queue;
HEREDOC

echo "6. give some time to consume the topic & replicate the data"

# that is slow :\
# TODO: smarter way to wait for consumer needed. Can we export some metric like maximum kafka lag, or use some external tools?

sleep 12

echo "7. check counts"

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT hostName() h, count(), uniqExact(id) FROM dummy GROUP BY h'

echo "8. cleanup"

cat <<HEREDOC | $DOCKER_COMPOSE exec -T clickhouse1 clickhouse client -n >/dev/null
DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS dummy_queue ON CLUSTER sharded_cluster_secure;
DROP TABLE IF EXISTS dummy_local ON CLUSTER sharded_cluster_secure;
DROP TABLE IF EXISTS dummy ON CLUSTER sharded_cluster_secure;
DROP TABLE IF EXISTS dummy_mv ON CLUSTER sharded_cluster_secure;
HEREDOC

$DOCKER_COMPOSE exec -T kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic dummytopic >/dev/null 2>&1
$DOCKER_COMPOSE exec -T kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group dummytopic_consumer_group1 >/dev/null 2>&1