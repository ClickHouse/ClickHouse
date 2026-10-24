#!/bin/bash
# Verify that the distroless keeper image starts under a uid/gid other than the image's own 101:101 (`docker run --user`).
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

cid="$(
  docker run -d \
    --user 102:102 \
    --name "$(cname)" \
    "$image"
)"
trap '[ $? -eq 0 ] || dumpServerLogs "$cid"; docker rm -vf "$cid" > /dev/null' EXIT

# Keeper listens on localhost only by default, so query it from inside its container.
# HOME: uid 102 has no passwd entry, and keeper-client writes its history file even with --query.
keeperCli() {
  docker exec -e HOME=/tmp "$cid" \
    clickhouse-keeper-client \
    --port 9181 \
    --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  keeperCli ruok

keeperCli 'create /custom_uid 1'
[ "$(keeperCli 'get /custom_uid')" = 1 ]
