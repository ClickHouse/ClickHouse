#!/bin/bash
# With `user_files_policy` configured, `user_files_path` is a directory inside the policy's disk rather
# than a local path, so the entrypoint must not prepare it as a local directory. The config sets
# `user_files_path` to `/dev/null/user_files/`, which cannot be created locally; the container must
# still come up, and `file` must use the directory `dev/null/user_files` on the policy's disk.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

cid="$(
  docker run -d \
    -v "$dir/user_files_policy.xml":/etc/clickhouse-server/config.d/user_files_policy.xml:ro \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf $cid > /dev/null' EXIT

chCli() {
  docker exec "$cid" clickhouse-client --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --cid "$cid" \
  --image "$image" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

# The policy's disk was prepared for the server's uid like every other `storage_configuration` disk.
server_uid="$(docker exec "$cid" sed -n 's/^Uid:[[:space:]]*\([0-9][0-9]*\).*/\1/p' /proc/1/status)"
[ -n "$server_uid" ]
[ "$server_uid" != 0 ]
docker exec -u "$server_uid" "$cid" test -d /var/lib/clickhouse/docker_test_user_files
[ "$(docker exec -u "$server_uid" "$cid" stat -c '%u' /var/lib/clickhouse/docker_test_user_files)" = "$server_uid" ]

# User files are served from `user_files_path` inside the policy's disk.
chCli "INSERT INTO FUNCTION file('numbers.csv', 'CSV', 'c1 UInt64') VALUES (1), (2), (3)"
docker exec -u "$server_uid" "$cid" test -f /var/lib/clickhouse/docker_test_user_files/dev/null/user_files/numbers.csv
[ "$(chCli "SELECT sum(c1) FROM file('numbers.csv', 'CSV', 'c1 UInt64')")" = 6 ]
