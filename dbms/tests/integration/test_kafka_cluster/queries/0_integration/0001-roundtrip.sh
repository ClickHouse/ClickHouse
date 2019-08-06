#!/usr/bin/env bash
DOCKER_COMPOSE=${DOCKER_COMPOSE:-docker-compose}

# docker-compose exec clickhouse1 clickhouse client
# docker-compose exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list
# docker-compose exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --list
# docker-compose exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group dummytopic_consumer_group1
echo "1. clean old topic & consumer group"

$DOCKER_COMPOSE exec kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic dummytopic >/dev/null 2>&1
$DOCKER_COMPOSE exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group dummytopic_consumer_group1 >/dev/null 2>&1

echo "2. create topic"

$DOCKER_COMPOSE exec kafka1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 12 --topic dummytopic

echo "3. create src table"

cat <<HEREDOC | $DOCKER_COMPOSE exec -T clickhouse1 clickhouse client -n
DROP TABLE IF EXISTS source_table;
CREATE TABLE source_table ENGINE = Log AS SELECT toUInt32(number) as id from numbers(240000);
HEREDOC

echo "4. copy table data to topic"

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT * FROM source_table FORMAT JSONEachRow' | $DOCKER_COMPOSE exec -T kafka1 kafka-console-producer --broker-list kafka1:9092,kafka2:9092,kafka3:9092 --topic dummytopic >/dev/null

echo "5. create dest tables pipeline"

cat <<HEREDOC | $DOCKER_COMPOSE exec -T clickhouse1 clickhouse client -n >/dev/null
DROP TABLE IF EXISTS dummy_queue ON CLUSTER replicated_cluster;
DROP TABLE IF EXISTS dummy ON CLUSTER replicated_cluster;
DROP TABLE IF EXISTS dummy_mv ON CLUSTER replicated_cluster;

CREATE TABLE dummy_queue ON CLUSTER replicated_cluster (
    id UInt32
) ENGINE = Kafka('kafka1:9092,kafka2:9092,kafka3:9092', 'dummytopic', 'dummytopic_consumer_group1', 'JSONEachRow');

CREATE TABLE dummy ON CLUSTER replicated_cluster (
    id UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}-{shard}/{table}', '{replica}') ORDER BY (id);

CREATE MATERIALIZED VIEW dummy_mv ON CLUSTER replicated_cluster TO dummy AS SELECT id FROM dummy_queue;
HEREDOC

echo "6. give some time to consume the topic & replicate the data"

# that is slow :\
# TODO: smarter way to wait for consumer needed. Can we export some metric like maximum kafka lag, or use some external tools?

sleep 12

echo "7. check counts"

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT count(), uniqExact(id) FROM dummy'

echo "8. stopping one kafka node (broker rebalance)"

$DOCKER_COMPOSE stop kafka2 >/dev/null 2>&1

echo "9. putting more data to kafka"

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT id+1000000 as id FROM source_table FORMAT JSONEachRow' | $DOCKER_COMPOSE exec -T kafka1 kafka-console-producer --broker-list kafka1:9092,kafka2:9092,kafka3:9092 --topic dummytopic >/dev/null 2>&1

echo "10. give some time to consume the topic & replicate the data"

# that is slow :\
# TODO: sleeps sucks. need smarter way to wait for consumer needed. Can we export some metric like maximum kafka lag, or use some external tools?

sleep 7

echo "11. check counts again"

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT count(), uniqExact(id) FROM dummy'

echo "12. starting kafka node back (broker rebalance #2)"

$DOCKER_COMPOSE start kafka2 >/dev/null 2>&1

echo "13. wait and check counts once again"

sleep 5

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT count(), uniqExact(id) FROM dummy'

#$DOCKER_COMPOSE exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group dummytopic_consumer_group1

echo "14. stopping one clickhouse node (consumer group rebalance)"

$DOCKER_COMPOSE stop clickhouse2 >/dev/null 2>&1

echo "15. putting more data to kafka"

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT id+2000000 as id FROM source_table FORMAT JSONEachRow' | $DOCKER_COMPOSE exec -T kafka1 kafka-console-producer --broker-list kafka1:9092,kafka2:9092,kafka3:9092 --topic dummytopic >/dev/null 2>&1

echo "16. give some time to consume the topic & replicate the data"

sleep 7

echo "17. check counts again"

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT count(), uniqExact(id) FROM dummy'

echo "18. starting clickhouse node back (consumer group rebalance #2)"

$DOCKER_COMPOSE start clickhouse2 >/dev/null 2>&1

echo "19. wait and check counts once again"

sleep 5

$DOCKER_COMPOSE exec -T clickhouse1 clickhouse client --query='SELECT count(), uniqExact(id) FROM dummy'

echo "20. cleanup"

cat <<HEREDOC | $DOCKER_COMPOSE exec -T clickhouse1 clickhouse client -n >/dev/null
DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS dummy_queue ON CLUSTER replicated_cluster;
DROP TABLE IF EXISTS dummy ON CLUSTER replicated_cluster;
DROP TABLE IF EXISTS dummy_mv ON CLUSTER replicated_cluster;
HEREDOC

$DOCKER_COMPOSE exec kafka1 kafka-topics --bootstrap-server localhost:9092 --delete --topic dummytopic >/dev/null
$DOCKER_COMPOSE exec kafka1 kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group dummytopic_consumer_group1 >/dev/null