#!/bin/bash
# An absolute `filesystem_caches` path below a directory the server does not own. The entrypoint
# prepares it, because the server cannot: `FileCache::initialize` calls `fs::create_directories` as
# the server's own uid, which has no capability to write into a `root:root` mount.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

# A destination-only `-v` gets an anonymous volume, which docker creates `root:root 0755` and
# `docker rm -v` reclaims. Dropping `CAP_DAC_OVERRIDE` is what makes this exercise the real failure
# mode, rather than passing because the process may write into that mount regardless of ownership.
cid="$(
  docker run -d \
    -v /mnt/clickhouse-cache \
    -v "$dir/cache.xml":/etc/clickhouse-server/config.d/cache.xml:ro \
    --cap-drop=DAC_OVERRIDE \
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

[ "$(chCli "SELECT path FROM system.filesystem_cache_settings WHERE cache_name = 'docker_test_cache'")" = /mnt/clickhouse-cache/docker_test_cache ]

# The uid comes from the server's own process, so a `chmod` in place of the `chown` would not pass.
server_uid="$(docker exec "$cid" sed -n 's/^Uid:[[:space:]]*\([0-9][0-9]*\).*/\1/p' /proc/1/status)"
[ -n "$server_uid" ]
[ "$server_uid" != 0 ]
docker exec -u "$server_uid" "$cid" test -d /mnt/clickhouse-cache/docker_test_cache
[ "$(docker exec -u "$server_uid" "$cid" stat -c '%u' /mnt/clickhouse-cache/docker_test_cache)" = "$server_uid" ]
