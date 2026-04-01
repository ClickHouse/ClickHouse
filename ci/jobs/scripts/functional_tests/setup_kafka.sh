#!/bin/bash

set -euxf -o pipefail

KAFKA_BROKER=${KAFKA_BROKER:-127.0.0.1:9092}

stop_redpanda() {
    if pgrep -f redpanda > /dev/null 2>&1; then
        echo "Stopping existing Redpanda process..."
        pkill -f redpanda || true
        sleep 2
        # Force kill if still running
        if pgrep -f redpanda > /dev/null 2>&1; then
            pkill -9 -f redpanda || true
            sleep 1
        fi
    fi
}

start_redpanda() {
    stop_redpanda
    rm -rf /tmp/redpanda-data

    echo "Starting Redpanda broker..."
    nohup rpk redpanda start \
        --mode dev-container \
        --smp 1 \
        --memory 256M \
        --reserve-memory 0M \
        --overprovisioned \
        --kafka-addr "127.0.0.1:9092" \
        --advertise-kafka-addr "127.0.0.1:9092" \
        --rpc-addr "127.0.0.1:33145" \
        --advertise-rpc-addr "127.0.0.1:33145" \
        --set redpanda.auto_create_topics_enabled=false \
        --set redpanda.log_segment_size=16777216 \
        > /tmp/redpanda.log 2>&1 &
    echo "Redpanda started with PID $!"
}

wait_for_redpanda() {
    local max_attempts=60
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if rpk topic list --brokers "$KAFKA_BROKER" > /dev/null 2>&1; then
            echo "Redpanda is ready"
            return 0
        fi
        echo "Waiting for Redpanda to be ready (attempt $((attempt + 1))/$max_attempts)..."
        sleep 1
        attempt=$((attempt + 1))
    done
    echo "ERROR: Redpanda failed to start within ${max_attempts} seconds"
    cat /tmp/redpanda.log || true
    return 1
}

main() {
    start_redpanda
    wait_for_redpanda
}

main "$@"
