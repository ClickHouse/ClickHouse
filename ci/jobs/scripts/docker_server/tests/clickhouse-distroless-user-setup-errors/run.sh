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
expectStartFailure 'failed to write /etc/clickhouse-server/users.d/default-user.xml' \
  --device /dev/full:/etc/clickhouse-server/users.d/default-user.xml -e CLICKHOUSE_PASSWORD=env_password
expectStartFailure 'error: cannot read CLICKHOUSE_PASSWORD_FILE' \
  -e CLICKHOUSE_USER=app -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/unreadable
expectStartFailure 'error: cannot read CLICKHOUSE_PASSWORD_FILE' \
  -e CLICKHOUSE_USER=app -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets

# Starts a server that must come up: user app logs in with password $1 and not without it, and the log contains $2.
expectStart() {
  local password="$1" message="$2" out
  shift 2
  local what="$*"
  cid="$(docker run -d --name "$(cname)" -v "$secrets":/run/test-secrets:ro -e CLICKHOUSE_USER=app "$@" "$image")"

  # shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
  . "$TESTS_LIB_DIR/retry.sh" \
    --tries "$CLICKHOUSE_TEST_TRIES" \
    --sleep "$CLICKHOUSE_TEST_SLEEP" \
    chCli SELECT 1

  if out="$(docker run --rm --link "$cid":clickhouse "$image" clickhouse-client --host clickhouse --user app --query 'SELECT 1' 2>&1)"; then
    echo "FAIL: user app accepted a login without its password ($what)" >&2
    exit 1
  fi
  grep -F AUTHENTICATION_FAILED <<< "$out" > /dev/null
  docker logs "$cid" 2>&1 | grep -F "$message" > /dev/null
  docker rm -vf "$cid" > /dev/null
  cid=''
}

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --user app \
    --password "$password" \
    --query "$*"
}

# Controls: a read-only root filesystem with a writable users.d applies the file's password, and a missing password
# file is ignored like in entrypoint.sh, so the CLICKHOUSE_PASSWORD password is applied.
expectStart file_password "create new user 'app' instead 'default'" \
  --read-only --tmpfs /tmp -v /etc/clickhouse-server/users.d \
  -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/readable
expectStart env_password "warning: cannot read CLICKHOUSE_PASSWORD_FILE '/run/test-secrets/missing': No such file or directory" \
  -e CLICKHOUSE_PASSWORD=env_password -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/missing
expectStart env_password "warning: cannot read CLICKHOUSE_PASSWORD_FILE '/run/test-secrets/readable/missing': Not a directory" \
  -e CLICKHOUSE_PASSWORD=env_password -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/readable/missing
