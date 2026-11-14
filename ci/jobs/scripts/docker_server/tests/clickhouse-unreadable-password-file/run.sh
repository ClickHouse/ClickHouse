#!/bin/bash
# Verify that the container does not start when `CLICKHOUSE_PASSWORD_FILE` cannot be read:
# otherwise the requested user would be created without a password and open to the network.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

# The directory is readable by the server user, the file is not.
secrets="$(mktemp -d)"
chmod 755 "$secrets"
echo 'unreadable_password' > "$secrets/password"
chmod 600 "$secrets/password"

cid="$(
  docker run -d \
    --user 101:101 \
    -e CLICKHOUSE_USER='unreadable_password_user' \
    -e CLICKHOUSE_PASSWORD_FILE=/run/secrets/password \
    -v "$secrets":/run/secrets:ro \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf "$cid" > /dev/null; rm -rf "$secrets"' EXIT

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
# It must fail because of the password file, not for an unrelated reason.
if ! docker logs "$cid" 2>&1 | grep -F '/run/secrets/password' > /dev/null; then
  echo "the container did not report the unreadable password file" >&2
  dumpServerLogs "$cid"
  exit 1
fi
