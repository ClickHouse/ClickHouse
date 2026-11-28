#!/bin/bash
# Verify that `clickhouse docker-init` initializes a server with only the native interface: with no `http_port`
# or `https_port` there is no `/ping` to wait for, so it waits for `SELECT 1` over the native interface instead.
# `entrypoint.sh` needs an HTTP port, so the Ubuntu image does not run this test.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

export CLICKHOUSE_USER='native_only_user'
export CLICKHOUSE_PASSWORD='native_only_password'
export CLICKHOUSE_DB='native_only_db'

cid="$(
  docker run -d \
    -e CLICKHOUSE_USER \
    -e CLICKHOUSE_PASSWORD \
    -e CLICKHOUSE_DB \
    -v "$dir/native-only.xml":/etc/clickhouse-server/config.d/native-only.xml:ro \
    --name "$(cname)" \
    "$image"
)"
trap '[ $? -eq 0 ] || dumpServerLogs "$cid"; docker rm -vf "$cid" > /dev/null' EXIT

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

chCli SHOW DATABASES | grep '^native_only_db$' > /dev/null
