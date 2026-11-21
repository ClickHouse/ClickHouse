#!/bin/bash
# Verify that the container does not start when a `*.sql.gz` init script cannot be decompressed, like
# `set -o pipefail` in `entrypoint.sh`: otherwise the client runs the empty input successfully, the server
# starts without the initialization, and the next starts skip it because the data directory is not empty.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

initdb="$(mktemp -d)"
chmod 755 "$initdb"
echo 'this is not gzip' > "$initdb/broken.sql.gz"
chmod 644 "$initdb/broken.sql.gz"

cid="$(
  docker run -d \
    -v "$initdb":/docker-entrypoint-initdb.d:ro \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf "$cid" > /dev/null; rm -rf "$initdb"' EXIT

# A server that started anyway keeps running, and `docker wait` does not return.
if ! exit_code="$(timeout 120 docker wait "$cid")"; then
  echo "the container is still running instead of failing" >&2
  dumpServerLogs "$cid"
  exit 1
fi
if [ "$exit_code" = 0 ]; then
  echo "the container exited with code 0 instead of failing" >&2
  dumpServerLogs "$cid"
  exit 1
fi
