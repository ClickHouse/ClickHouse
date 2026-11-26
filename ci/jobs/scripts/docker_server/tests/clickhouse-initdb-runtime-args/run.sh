#!/bin/bash
# Verify the user and database setup with the inputs entrypoint.sh supports:
# - a user name that is a valid XML element name but not an SQL identifier (`service-account`),
# - a password file with trailing newlines: the whole file without them is the password, as with `$(cat file)`,
# - `CLICKHOUSE_DB` together with a `tcp_port` overridden by the server arguments: the database is
#   created through the port the temporary server actually listens on.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

secrets="$(mktemp -d)"
chmod 755 "$secrets"
printf 'secret token\n\n' > "$secrets/password"
chmod 644 "$secrets/password"

cid="$(
  docker run -d \
    -e CLICKHOUSE_USER='service-account' \
    -e CLICKHOUSE_PASSWORD_FILE=/run/secrets/password \
    -e CLICKHOUSE_DB='appdb' \
    -v "$secrets":/run/secrets:ro \
    --name "$(cname)" \
    "$image" \
    -- --tcp_port=19001
)"
trap '[ $? -eq 0 ] || dumpServerLogs "$cid"; docker rm -vf "$cid" > /dev/null; rm -rf "$secrets"' EXIT

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --port 19001 \
    --user 'service-account' \
    --password 'secret token' \
    --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

chCli SHOW DATABASES | grep '^appdb$' > /dev/null

if docker run --rm --link "$cid":clickhouse "$image" \
    clickhouse-client --host clickhouse --port 19001 --user 'service-account' --password '' \
    --query 'SELECT 1' > /dev/null 2>&1; then
  echo "an empty password is accepted" >&2
  exit 1
fi
