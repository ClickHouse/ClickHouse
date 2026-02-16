#!/bin/bash

set -euxf -o pipefail

KAFKA_BROKER=${KAFKA_BROKER:-127.0.0.1:9092}

write_kafka_config() {
    local config_file="$1"
    cat > "$config_file" <<EOF
# KRaft mode (no ZooKeeper dependency)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

log.dirs=/tmp/kafka-logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
log.retention.hours=1
EOF
}

start_kafka() {
    local config_file="/tmp/kafka-server.properties"
    write_kafka_config "$config_file"

    rm -rf /tmp/kafka-logs

    export KAFKA_HEAP_OPTS="-Xmx256m -Xms256m"

    # Format storage for KRaft mode
    local cluster_id
    cluster_id=$(kafka-storage.sh random-uuid)
    kafka-storage.sh format --config "$config_file" --cluster-id "$cluster_id"

    echo "Starting Kafka broker in KRaft mode..."
    nohup kafka-server-start.sh "$config_file" > /tmp/kafka.log 2>&1 &
    echo "Kafka started with PID $!"
}

wait_for_kafka() {
    local max_attempts=60
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --list > /dev/null 2>&1; then
            echo "Kafka is ready"
            return 0
        fi
        echo "Waiting for Kafka to be ready (attempt $((attempt + 1))/$max_attempts)..."
        sleep 1
        attempt=$((attempt + 1))
    done
    echo "ERROR: Kafka failed to start within ${max_attempts} seconds"
    cat /tmp/kafka.log || true
    return 1
}

main() {
    start_kafka
    wait_for_kafka
}

main "$@"
