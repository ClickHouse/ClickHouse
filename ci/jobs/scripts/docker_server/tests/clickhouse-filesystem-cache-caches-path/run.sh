#!/bin/bash
# A relative `filesystem_caches` path with an explicit `filesystem_caches_path`.
# Check successful startup and ownership of the path chosen by the server.
# Do not assert a location: this must also pass when the server's initialization
# ordering is corrected together with any required directory preparation.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

# Docker creates a fresh anonymous volume owned by `root:root` with mode `0755`.
# Without `CAP_DAC_OVERRIDE`, the server cannot create a cache there until the
# entrypoint has prepared its directory or a writable parent.
cid="$(
  docker run -d \
    -v /mnt/clickhouse-cache \
    -v "$dir/cache.xml":/etc/clickhouse-server/config.d/cache.xml:ro \
    --cap-drop=DAC_OVERRIDE \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf "$cid" > /dev/null' EXIT

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

cache_path="$(chCli "SELECT path FROM system.filesystem_cache_settings WHERE cache_name = 'docker_relative_cache'")"
[ -n "$cache_path" ]

server_uid="$(docker exec "$cid" sed -n 's/^Uid:[[:space:]]*\([0-9][0-9]*\).*/\1/p' /proc/1/status)"
[ -n "$server_uid" ]
[ "$server_uid" != 0 ]

# Inspected as the server's own uid, not as root: the server creates `<path>/caches` as
# `drwxr-x---`, so with `CAP_DAC_OVERRIDE` dropped root cannot traverse into it and a directory
# that exists would read as missing.
docker exec -u "$server_uid" "$cid" test -d "$cache_path"
[ "$(docker exec -u "$server_uid" "$cid" stat -c '%u' "$cache_path")" = "$server_uid" ]
