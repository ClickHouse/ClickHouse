#!/bin/bash
# Verify that the container does not start when the entrypoint cannot write the requested user configuration:
# otherwise the server would run with the shipped `default` user, without the password and open to the network.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

# Readable by the server, which runs as another user, so that only writing to it fails.
users_d="$(mktemp -d)"
chmod 755 "$users_d"

cid="$(
  docker run -d \
    -e CLICKHOUSE_PASSWORD='readonly_users_d_password' \
    -v "$users_d":/etc/clickhouse-server/users.d:ro \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf "$cid" > /dev/null; rm -rf "$users_d"' EXIT

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
