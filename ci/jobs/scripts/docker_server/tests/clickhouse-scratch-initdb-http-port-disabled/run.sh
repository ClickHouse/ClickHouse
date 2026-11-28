#!/bin/bash
# Verify that the initialization completes when the server arguments disable the HTTP interface (`-- --http_port=`):
# the temporary server gets the same arguments and opens no HTTP listener, so `clickhouse docker-init` must not wait
# for the HTTP port of the configuration file, but check the readiness on the native port. `entrypoint.sh` of the
# Ubuntu image starts the temporary server differently, so the test runs only on the `FROM scratch` image.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

cid="$(
  docker run -d \
    -e CLICKHOUSE_DB=http_port_disabled_db \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    --name "$(cname)" \
    "$image" \
    -- --http_port=
)"
trap '[ $? -eq 0 ] || dumpServerLogs "$cid"; docker rm -vf "$cid" > /dev/null' EXIT

# The database exists only if the initialization completed; then the final server runs with the same arguments.
checkInitialized() {
  [ "$(docker inspect -f '{{.State.Running}}' "$cid")" = true ] || { echo "the container stopped" >&2; exit 1; }
  [ "$(docker exec "$cid" clickhouse-client --query "SELECT count() FROM system.databases WHERE name = 'http_port_disabled_db'")" = 1 ]
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries 40 \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  checkInitialized
