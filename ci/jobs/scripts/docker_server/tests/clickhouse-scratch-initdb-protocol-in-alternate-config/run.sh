#!/bin/bash
# Verify that the temporary server of the initialization accepts only local connections on a protocol with its own
# `host` when the server arguments select another configuration file: `-- --config-file=<path>`. The file named by
# `CLICKHOUSE_CONFIG` (the default `config.xml`) has no such protocol, so `clickhouse docker-init` has to read
# the protocols from the file the server loads. `entrypoint.sh` of the Ubuntu image does not do that, so the test
# runs only on the `FROM scratch` image.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"
name="$(cname)"
protocol_dir="$dir/../clickhouse-scratch-initdb-protocol-in-config"

# The alternate file is the image's own `config.xml`; the protocol is merged into it from `alternate.d`,
# which the default `config.xml` does not read.
config="$(mktemp -d)"
config_cid="$(docker create "$image")"
cid=
trap '[ $? -eq 0 ] || [ -z "$cid" ] || dumpServerLogs "$cid"; docker rm -vf "$config_cid" $cid > /dev/null; rm -rf "$config"' EXIT
docker cp "$config_cid:/etc/clickhouse-server/config.xml" "$config/alternate.xml"
chmod -R a+rX "$config"

cid="$(
  docker run -d \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -v "$protocol_dir/initdb.sql":/docker-entrypoint-initdb.d/initdb.sql:ro \
    -v "$config/alternate.xml":/etc/clickhouse-server/alternate.xml:ro \
    -v "$protocol_dir/bootstrap-protocol.xml":/etc/clickhouse-server/alternate.d/bootstrap-protocol.xml:ro \
    --name "$name" \
    "$image" \
    -- --config-file=/etc/clickhouse-server/alternate.xml
)"

remoteQuery() {
  docker run --rm --link "$cid":clickhouse "$image" \
    clickhouse-client --host clickhouse --port 19003 --connect_timeout 2 --query "$*"
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
  echo "the temporary server accepts remote connections on a protocol from the configuration file selected by the server arguments during the initialization" >&2
  exit 1
fi
if [ "$(docker inspect -f '{{.State.Running}}' "$cid")" != true ]; then
  echo "the container stopped during the initialization" >&2
  exit 1
fi

# Afterwards, the server listens on the network, as the configuration requests: this also shows that the protocol
# is defined in the file the server loads.
# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries 20 \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  remoteQuery SELECT 1
