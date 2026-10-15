#!/bin/bash

set -euxf -o pipefail

KAFKA_BROKER=${KAFKA_BROKER:-127.0.0.1:9092}
SCHEMA_REGISTRY_ADDR=${SCHEMA_REGISTRY_ADDR:-127.0.0.1:8081}
# 19092 sits outside the Linux ephemeral port range (default 32768-60999),
# so the kernel won't hand it out as a source port and leave it in TIME_WAIT
# right when we try to bind. Keep it in sync with --rpc-addr below.
RPC_PORT=${RPC_PORT:-19092}

preflight() {
    # Kill any leftover redpanda from a previous job that didn't reach teardown.
    pkill -9 -f 'rpk redpanda start' || true
    pkill -9 -f '/opt/redpanda/' || true

    # Surface what's bound on our ports, then refuse to start if any are busy.
    local ports=("9092" "8081" "${RPC_PORT}")
    local busy=0
    for p in "${ports[@]}"; do
        if ss -tlnH "sport = :${p}" | grep -q .; then
            echo "ERROR: port ${p} is already in use:"
            ss -tlnpH "sport = :${p}" || true
            busy=1
        fi
    done
    # TIME_WAIT on the RPC port also prevents bind; report it for diagnosis.
    ss -tanH "sport = :${RPC_PORT}" state time-wait || true
    if [ "$busy" -ne 0 ]; then
        echo "ERROR: cannot start Redpanda, required ports are occupied"
        return 1
    fi
}

start_redpanda() {
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
        --rpc-addr "127.0.0.1:${RPC_PORT}" \
        --advertise-rpc-addr "127.0.0.1:${RPC_PORT}" \
        --schema-registry-addr "${SCHEMA_REGISTRY_ADDR}" \
        --set redpanda.auto_create_topics_enabled=false \
        --set redpanda.log_segment_size=16777216 \
        > /tmp/redpanda.log 2>&1 &
    REDPANDA_PID=$!
    echo "Redpanda started with PID ${REDPANDA_PID}"
}

wait_for_redpanda() {
    local max_attempts=60
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        # If the broker process died, fail fast instead of polling for 60s.
        if [ -n "${REDPANDA_PID:-}" ] && ! kill -0 "${REDPANDA_PID}" 2>/dev/null; then
            echo "ERROR: Redpanda process ${REDPANDA_PID} exited during startup"
            cat /tmp/redpanda.log || true
            return 1
        fi
        if rpk topic list --brokers "$KAFKA_BROKER" > /dev/null 2>&1 \
            && curl -sf "http://${SCHEMA_REGISTRY_ADDR}/subjects" > /dev/null 2>&1; then
            echo "Redpanda is ready (broker + schema registry)"
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
    preflight
    start_redpanda
    wait_for_redpanda
}

main "$@"
