#!/bin/bash
# Verify that the temporary server of the initialization accepts only local connections, even when the server
# arguments override `listen_host`: an init script may be what sets the password, as here with
# `CLICKHOUSE_SKIP_USER_SETUP=1`, and until it is done the server must not be reachable from the network.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

cid="$(
  docker run -d \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -v "$dir/initdb.sql":/docker-entrypoint-initdb.d/initdb.sql:ro \
    --name "$(cname)" \
    "$image" \
    -- --listen_host=0.0.0.0
)"
trap '[ $? -eq 0 ] || dumpServerLogs "$cid"; docker rm -vf "$cid" > /dev/null' EXIT

remoteQuery() {
  docker run --rm --link "$cid":clickhouse "$image" \
    clickhouse-client --host clickhouse --connect_timeout 2 --query "$*"
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
