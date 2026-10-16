#!/bin/bash
# docker-init must not start the server when it cannot apply the requested user setup, like entrypoint.sh under `set -e`.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

secrets="$(mktemp -d)"
chmod 755 "$secrets"
echo 'file_password' > "$secrets/readable"
chmod 644 "$secrets/readable"
echo 'file_password' > "$secrets/unreadable"
chmod 000 "$secrets/unreadable"

cid=''
trap '[ $? -eq 0 ] || { [ -z "$cid" ] || dumpServerLogs "$cid"; }; [ -z "$cid" ] || docker rm -vf "$cid" > /dev/null; rm -rf "$secrets"' EXIT

expectStartFailure() {
  local message="$1" code
  shift
  cid="$(docker run -d --name "$(cname)" -v "$secrets":/run/test-secrets:ro "$@" "$image")"
  if ! code="$(timeout 120 docker wait "$cid")"; then
    echo "FAIL: the server started although the user setup failed ($*)" >&2
    exit 1
  fi
  [ "$code" != 0 ]
  docker logs "$cid" 2>&1 | grep -F "$message" > /dev/null
  docker rm -vf "$cid" > /dev/null
  cid=''
}

expectStartFailure 'failed to write /etc/clickhouse-server/users.d/default-user.xml' \
  --read-only --tmpfs /tmp -e CLICKHOUSE_PASSWORD=env_password
expectStartFailure 'failed to write /etc/clickhouse-server/users.d/default-user.xml' \
  --read-only --tmpfs /tmp
expectStartFailure 'cannot read CLICKHOUSE_PASSWORD_FILE' \
  -e CLICKHOUSE_USER=app -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/unreadable

# Control: the same image on a read-only root filesystem starts once users.d is writable and applies the file's password.
cid="$(
  docker run -d --name "$(cname)" \
    --read-only --tmpfs /tmp -v /etc/clickhouse-server/users.d \
    -v "$secrets":/run/test-secrets:ro \
    -e CLICKHOUSE_USER=app -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/readable \
    "$image"
)"

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --user app \
    --password file_password \
    --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

if out="$(docker run --rm --link "$cid":clickhouse "$image" clickhouse-client --host clickhouse --user app --query 'SELECT 1' 2>&1)"; then
  echo "FAIL: user app accepted a login without its password" >&2
  exit 1
fi
grep -F AUTHENTICATION_FAILED <<< "$out" > /dev/null
