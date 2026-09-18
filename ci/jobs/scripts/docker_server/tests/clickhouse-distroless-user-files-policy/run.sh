#!/bin/bash
# The distroless image starts through the compiled `clickhouse docker-init` entrypoint, which must
# apply the same rule as `entrypoint.sh`: with `user_files_policy` configured the legacy
# `user_files_path` is not prepared, so an unusable one (here a path under `/dev/null`) must not stop
# the container. Shares the config of the `clickhouse-user-files-policy` test.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

cid="$(
  docker run -d \
    -v "$dir/../clickhouse-user-files-policy/user_files_policy.xml":/etc/clickhouse-server/config.d/user_files_policy.xml:ro \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf $cid > /dev/null' EXIT

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

# The server is up although its legacy `user_files_path` cannot exist, and user files are served
# from the policy's disk, which the entrypoint prepared as a `storage_configuration` disk.
[ "$(chCli "SELECT value FROM system.server_settings WHERE name = 'user_files_policy'")" = docker_test_user_files ]
chCli "INSERT INTO FUNCTION file('numbers.csv', 'CSV', 'c1 UInt64') VALUES (1), (2), (3)"
[ "$(chCli "SELECT sum(c1) FROM file('numbers.csv', 'CSV', 'c1 UInt64')")" = 6 ]
