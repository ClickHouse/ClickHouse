#!/bin/bash
# Verify that the temporary server of the initialization accepts only local connections, even when the server
# arguments override `listen_host`, add a repeated `listen_host[1]`, a protocol with its own `host`, or the streaming
# exchange of distributed queries with its own `streaming_exchange_listen_host`: an init script may be what sets the password, as here with
# `CLICKHOUSE_SKIP_USER_SETUP=1`, and until it is done the server must not be reachable from the network.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"
name="$(cname)"

cid="$(
  docker run -d \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -v "$dir/initdb.sql":/docker-entrypoint-initdb.d/initdb.sql:ro \
    --name "$name" \
    --hostname "$name" \
    "$image" \
    -- --listen_host=0.0.0.0 --listen_host[1]="$name" \
    --protocols.bootstrap.type=tcp --protocols.bootstrap.host=0.0.0.0 --protocols.bootstrap.port=19002 \
    --distributed_query.streaming_exchange_port=19004 --distributed_query.streaming_exchange_listen_host=0.0.0.0
)"
trap '[ $? -eq 0 ] || dumpServerLogs "$cid"; docker rm -vf "$cid" > /dev/null' EXIT

remoteQueryOnPort() {
  local port="$1"
  shift
  docker run --rm --link "$cid":clickhouse "$image" \
    clickhouse-client --host clickhouse --port "$port" --connect_timeout 2 --query "$*"
}

remoteQuery() {
  remoteQueryOnPort 9000 "$@"
}

# Whether the streaming exchange port accepts TCP connections from another container. It speaks its own protocol, so
# the client fails on it anyway, but only a closed port fails with `Connection refused`.
exchangeOpen() {
  local out
  # Short timeouts: an open exchange port may not answer the client's hello at all.
  out="$(timeout 30 docker run --rm --link "$cid":clickhouse "$image" \
    clickhouse-client --host clickhouse --port 19004 --connect_timeout 2 --receive_timeout 2 --send_timeout 2 \
    --query 'SELECT 1' 2>&1 || true)"
  ! grep -F 'Connection refused' > /dev/null <<< "$out"
}

# Wait until the init script runs: its slow query keeps the temporary server up.
for _ in $(seq 1 120); do
  docker logs "$cid" 2>&1 | grep -F 'initdb.sql' > /dev/null && break
  [ "$(docker inspect -f '{{.State.Running}}' "$cid")" = true ] || { echo "the container stopped" >&2; exit 1; }
  sleep 1
done
docker logs "$cid" 2>&1 | grep -F 'initdb.sql' > /dev/null

# The query runs for 30 seconds, so the temporary server is still the one serving here.
if remoteQuery 'SELECT 1' > /dev/null 2>&1; then
  echo "the temporary server accepts remote connections during the initialization" >&2
  exit 1
fi
if remoteQueryOnPort 19002 'SELECT 1' > /dev/null 2>&1; then
  echo "the temporary server accepts remote connections on a custom protocol during the initialization" >&2
  exit 1
fi
if exchangeOpen; then
  echo "the temporary server accepts remote connections on the streaming exchange port during the initialization" >&2
  exit 1
fi
if [ "$(docker inspect -f '{{.State.Running}}' "$cid")" != true ]; then
  echo "the container stopped during the initialization" >&2
  exit 1
fi

# Afterwards, the server listens on the network, as the arguments request.
# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries 20 \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  remoteQuery SELECT 1
remoteQueryOnPort 19002 SELECT 1
# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries 20 \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  exchangeOpen
