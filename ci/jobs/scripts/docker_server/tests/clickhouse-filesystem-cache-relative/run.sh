#!/bin/bash
# A relative `filesystem_caches` path, with `filesystem_caches_path` unset as in the shipped image,
# so the server resolves it under `<path>/caches`. The prefix is the server's to choose either way,
# so the entrypoint must leave the entry alone: preparing the value as written would make a
# directory beside the one the server actually opens.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

cid="$(
  docker run -d \
    -v "$dir/cache.xml":/etc/clickhouse-server/config.d/cache.xml:ro \
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

data_dir="$(chCli "SELECT value FROM system.server_settings WHERE name = 'path'")"
[ "$(chCli "SELECT path FROM system.filesystem_cache_settings WHERE cache_name = 'docker_relative_cache'")" = "${data_dir}caches/docker_relative_cache" ]

# The entrypoint runs `cd "$DATA_DIR"` before `manage_clickhouse_directories`, regardless of
# the image's `WORKDIR`. Without `grep '^/'`, it creates the raw relative path here,
# beside `caches`, so this assertion must fail when that filter is removed.
docker exec "$cid" test ! -e "${data_dir}docker_relative_cache"
