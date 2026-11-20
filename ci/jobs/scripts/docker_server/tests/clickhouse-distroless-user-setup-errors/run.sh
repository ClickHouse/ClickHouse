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
mkdir -m 755 "$secrets/users.d"
echo '<clickhouse><users><default><password>users_d_password</password></default></users></clickhouse>' > "$secrets/users.d/default-password.xml"
chmod 644 "$secrets/users.d/default-password.xml"

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

# Starts a server that must come up: user $1 logs in with password $2 and not without it, and the log contains $3.
expectStart() {
  local user="$1" password="$2" message="$3" out
  shift 3
  local what="$*"
  cid="$(docker run -d --name "$(cname)" -v "$secrets":/run/test-secrets:ro "$@" "$image")"

  # shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
  . "$TESTS_LIB_DIR/retry.sh" \
    --tries "$CLICKHOUSE_TEST_TRIES" \
    --sleep "$CLICKHOUSE_TEST_SLEEP" \
    chCli SELECT 1

  if out="$(docker run --rm --link "$cid":clickhouse "$image" clickhouse-client --host clickhouse --user "$user" --query 'SELECT 1' 2>&1)"; then
    echo "FAIL: user $user accepted a login without its password ($what)" >&2
    exit 1
  fi
  grep -F AUTHENTICATION_FAILED <<< "$out" > /dev/null
  [ -z "$message" ] || docker logs "$cid" 2>&1 | grep -F "$message" > /dev/null
  docker rm -vf "$cid" > /dev/null
  cid=''
}

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --user "$user" \
    --password "$password" \
    --query "$*"
}

# Controls: a read-only root filesystem with a writable users.d applies the file's password, and a missing password
# file is ignored like in entrypoint.sh, so the CLICKHOUSE_PASSWORD password is applied.
expectStart app file_password "create new user 'app' instead 'default'" \
  -e CLICKHOUSE_USER=app --read-only --tmpfs /tmp -v /etc/clickhouse-server/users.d \
  -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/readable
expectStart app env_password "warning: cannot read CLICKHOUSE_PASSWORD_FILE '/run/test-secrets/missing': No such file or directory" \
  -e CLICKHOUSE_USER=app -e CLICKHOUSE_PASSWORD=env_password -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/missing
expectStart app env_password "warning: cannot read CLICKHOUSE_PASSWORD_FILE '/run/test-secrets/readable/missing': Not a directory" \
  -e CLICKHOUSE_USER=app -e CLICKHOUSE_PASSWORD=env_password -e CLICKHOUSE_PASSWORD_FILE=/run/test-secrets/readable/missing
# Without any CLICKHOUSE_* variable, a user `default` configured in a read-only users.d mount is left as it is.
expectStart default users_d_password '' \
  -v "$secrets/users.d":/etc/clickhouse-server/users.d:ro
