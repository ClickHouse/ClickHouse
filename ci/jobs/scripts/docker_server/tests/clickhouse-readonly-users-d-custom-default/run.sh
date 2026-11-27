#!/bin/bash
# Verify that the container starts with a read-only `users.d` that customises the `default` user, without any
# `CLICKHOUSE_*` user settings: the entrypoint must detect the customisation and leave the user as configured,
# instead of trying to write its own `default-user.xml` there.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

users_d="$(mktemp -d)"
chmod 755 "$users_d"
cat > "$users_d/default-password.xml" <<'XML'
<clickhouse>
  <users>
    <default>
      <password>custom_default_password</password>
      <networks replace="replace">
        <ip>::/0</ip>
      </networks>
    </default>
  </users>
</clickhouse>
XML
chmod 644 "$users_d/default-password.xml"

cid="$(
  docker run -d \
    -v "$users_d":/etc/clickhouse-server/users.d:ro \
    --name "$(cname)" \
    "$image"
)"
trap '[ $? -eq 0 ] || dumpServerLogs "$cid"; docker rm -vf "$cid" > /dev/null; rm -rf "$users_d"' EXIT

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --password custom_default_password \
    --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1
