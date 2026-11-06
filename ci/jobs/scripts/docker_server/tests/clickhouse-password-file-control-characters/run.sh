#!/bin/bash
# Verify that the container does not start with a password file whose secret has a line break before its end,
# here an empty first line: `clickhouse-client` cannot log in with such a password, and taking only
# the first line would create the requested user with an empty password, open to the network.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

secrets="$(mktemp -d)"
chmod 755 "$secrets"
printf '\nsecret_token\n' > "$secrets/password"
chmod 644 "$secrets/password"

cid="$(
  docker run -d \
    -e CLICKHOUSE_USER='service' \
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
# It must fail because of the password, not for an unrelated reason.
if ! docker logs "$cid" 2>&1 | grep -F 'contains ASCII control characters' > /dev/null; then
  echo "the container did not report the password with control characters" >&2
  dumpServerLogs "$cid"
  exit 1
fi
