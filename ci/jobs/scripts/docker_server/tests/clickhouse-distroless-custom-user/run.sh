#!/bin/bash
# Verify that the distroless image starts under a uid/gid other than the image's own 101:101 (`docker run --user`).
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

export CLICKHOUSE_USER='custom_uid_user'
export CLICKHOUSE_PASSWORD='custom_uid_password'

cid="$(
  docker run -d \
    --user 102:102 \
    -e CLICKHOUSE_USER \
    -e CLICKHOUSE_PASSWORD \
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

# The user only exists if docker-init could write users.d; the table needs a writable data directory.
chCli 'CREATE TABLE custom_uid (x UInt8) ENGINE = MergeTree ORDER BY x'
chCli 'INSERT INTO custom_uid VALUES (1)'
[ "$(chCli 'SELECT sum(x) FROM custom_uid')" = 1 ]
