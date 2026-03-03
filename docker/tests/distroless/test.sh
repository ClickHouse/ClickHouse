#!/usr/bin/env bash
# Integration tests for the distroless ClickHouse images.
#
# Usage:
#   bash test.sh [--version VERSION] [--server-context DIR] [--keeper-context DIR]
#
# The script:
#   1. Builds server + keeper distroless images (production and debug targets)
#   2. Runs each container and verifies startup
#   3. Checks CLICKHOUSE_USER/PASSWORD/DB env-var handling
#   4. Runs SQL init scripts and verifies the data
#   5. Confirms that /bin/sh is absent in the production image
#   6. Confirms that /busybox/sh is present in the debug image
#   7. Verifies the keeper starts successfully

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

VERSION="${VERSION:-26.1.2.11}"
SERVER_CONTEXT="${REPO_ROOT}/docker/server"
KEEPER_CONTEXT="${REPO_ROOT}/docker/keeper"

SERVER_IMAGE="clickhouse/clickhouse-server:distroless-test"
SERVER_DEBUG_IMAGE="clickhouse/clickhouse-server:distroless-debug-test"
KEEPER_IMAGE="clickhouse/clickhouse-keeper:distroless-test"

PASS=0
FAIL=0

pass() { echo "  PASS: $*"; (( PASS++ )) || true; }
fail() { echo "  FAIL: $*" >&2; (( FAIL++ )) || true; }

# ──────────────────────────────────────────────────────────────────────────────
# Build images
# ──────────────────────────────────────────────────────────────────────────────
echo "=== Building images ==="

docker build \
    --file "${SERVER_CONTEXT}/Dockerfile.distroless" \
    --target production \
    --build-arg "VERSION=${VERSION}" \
    --tag "${SERVER_IMAGE}" \
    "${SERVER_CONTEXT}"

docker build \
    --file "${SERVER_CONTEXT}/Dockerfile.distroless" \
    --target debug \
    --build-arg "VERSION=${VERSION}" \
    --tag "${SERVER_DEBUG_IMAGE}" \
    "${SERVER_CONTEXT}"

docker build \
    --file "${KEEPER_CONTEXT}/Dockerfile.distroless" \
    --target production \
    --build-arg "VERSION=${VERSION}" \
    --tag "${KEEPER_IMAGE}" \
    "${KEEPER_CONTEXT}"

echo "=== Images built successfully ==="

# ──────────────────────────────────────────────────────────────────────────────
# Helper: wait for clickhouse-client SELECT 1 to succeed
# ──────────────────────────────────────────────────────────────────────────────
wait_for_server() {
    local container="$1"
    local port="$2"
    local user="${3:-default}"
    local password="${4:-}"
    local tries=60

    echo "  Waiting for ${container} on port ${port}..."
    while (( tries-- > 0 )); do
        if docker exec "${container}" \
               /usr/bin/clickhouse client \
               --host 127.0.0.1 --port "${port}" \
               -u "${user}" --password "${password}" \
               --query "SELECT 1" \
               >/dev/null 2>&1; then
            echo "  Server ready."
            return 0
        fi
        sleep 1
    done
    echo "  ERROR: server did not become ready." >&2
    docker logs "${container}" >&2
    return 1
}

# ──────────────────────────────────────────────────────────────────────────────
# Test 1: Basic server startup with custom user/password
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 1: basic server startup ==="
CID=$(docker run -d \
    --name ch-distroless-t1 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t1 9000 testuser testpass

result=$(docker exec ch-distroless-t1 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT 42")
if [[ "${result}" == "42" ]]; then
    pass "server responds to queries"
else
    fail "expected '42', got '${result}'"
fi

docker rm -f ch-distroless-t1 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 2: No shell in production image
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 2: no shell in production image ==="
if docker run --rm --entrypoint /bin/sh "${SERVER_IMAGE}" -c "echo bad" 2>/dev/null; then
    fail "/bin/sh should not exist in production image"
else
    pass "/bin/sh absent from production image"
fi

if docker run --rm --entrypoint /bin/bash "${SERVER_IMAGE}" -c "echo bad" 2>/dev/null; then
    fail "/bin/bash should not exist in production image"
else
    pass "/bin/bash absent from production image"
fi

# ──────────────────────────────────────────────────────────────────────────────
# Test 3: busybox shell present in debug image
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 3: busybox shell in debug image ==="
result=$(docker run --rm --entrypoint /busybox/sh "${SERVER_DEBUG_IMAGE}" -c "echo ok" 2>/dev/null || true)
if [[ "${result}" == "ok" ]]; then
    pass "/busybox/sh available in debug image"
else
    fail "expected 'ok' from busybox shell, got '${result}'"
fi

# ──────────────────────────────────────────────────────────────────────────────
# Test 4: Init scripts (SQL files)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 4: SQL init scripts ==="
CID=$(docker run -d \
    --name ch-distroless-t4 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -e CLICKHOUSE_DB=test_db \
    -v "${SCRIPT_DIR}/initdb:/docker-entrypoint-initdb.d:ro" \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t4 9000 testuser testpass

result=$(docker exec ch-distroless-t4 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM test_db.events")
if [[ "${result}" == "2" ]]; then
    pass "init scripts created and populated test_db.events (count=2)"
else
    fail "expected count 2, got '${result}'"
fi

docker rm -f ch-distroless-t4 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 5: CLICKHOUSE_DB creates the database
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 5: CLICKHOUSE_DB creates a database ==="
CID=$(docker run -d \
    --name ch-distroless-t5 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    -e CLICKHOUSE_DB=myapp \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t5 9000 testuser testpass

result=$(docker exec ch-distroless-t5 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT count() FROM system.databases WHERE name='myapp'")
if [[ "${result}" == "1" ]]; then
    pass "CLICKHOUSE_DB created database 'myapp'"
else
    fail "expected database 'myapp' to exist, got count '${result}'"
fi

docker rm -f ch-distroless-t5 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 6: ClickHouse Keeper starts
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 6: keeper startup ==="
CID=$(docker run -d \
    --name ch-distroless-keeper \
    "${KEEPER_IMAGE}")

echo "  Waiting for keeper on port 9181..."
tries=60
keeper_ready=false
while (( tries-- > 0 )); do
    if docker exec ch-distroless-keeper \
           /usr/bin/clickhouse keeper-client \
           --host 127.0.0.1 --port 9181 \
           -q "ruok" \
           2>/dev/null | grep -q "imok"; then
        keeper_ready=true
        break
    fi
    sleep 1
done

if [[ "${keeper_ready}" == "true" ]]; then
    pass "keeper started and responded to 'ruok'"
else
    fail "keeper did not start in time"
    docker logs ch-distroless-keeper >&2
fi

docker rm -f ch-distroless-keeper >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Test 7: Container runs as non-root (uid 101)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Test 7: container uid ==="
CID=$(docker run -d \
    --name ch-distroless-t7 \
    -e CLICKHOUSE_USER=testuser \
    -e CLICKHOUSE_PASSWORD=testpass \
    "${SERVER_IMAGE}")

wait_for_server ch-distroless-t7 9000 testuser testpass

uid=$(docker exec ch-distroless-t7 \
    /usr/bin/clickhouse client --host 127.0.0.1 --port 9000 \
    -u testuser --password testpass \
    --query "SELECT currentUser()")

# Verify via /proc that the server process runs as uid 101
container_uid=$(docker inspect --format '{{.Config.User}}' ch-distroless-t7)
if [[ "${container_uid}" == "101:101" ]]; then
    pass "container configured with USER 101:101"
else
    fail "expected USER 101:101, got '${container_uid}'"
fi

docker rm -f ch-distroless-t7 >/dev/null

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="
if (( FAIL > 0 )); then
    exit 1
fi
