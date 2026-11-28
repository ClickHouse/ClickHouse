#!/bin/bash
# Verify that the init scripts run when the password of `default` comes from a mounted read-only `users.d`, without
# any `CLICKHOUSE_*` user settings, and an init script passes that password itself: the entrypoint must not need
# the credentials from the environment to see that the temporary server is ready, as `entrypoint.sh` uses `/ping`.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

users_d="$(mktemp -d)"
initdb="$(mktemp -d)"
shell_dir=""
busybox_cid=""
cid=""
trap '[ $? -eq 0 ] || { [ -z "$cid" ] || dumpServerLogs "$cid"; }; [ -z "$cid" ] || docker rm -vf "$cid" > /dev/null; rm -rf "$users_d" "$initdb" ${shell_dir:+"$shell_dir"}' EXIT

chmod 755 "$users_d" "$initdb"
cat > "$users_d/default-password.xml" <<'XML'
<clickhouse>
  <users>
    <default>
      <password>mounted_default_password</password>
      <networks replace="replace">
        <ip>::/0</ip>
      </networks>
    </default>
  </users>
</clickhouse>
XML
chmod 644 "$users_d/default-password.xml"

cat > "$initdb/init.sh" <<'SCRIPT'
#!/bin/sh
set -e
clickhouse client --password mounted_default_password --query 'CREATE DATABASE mounted_password_db'
SCRIPT
chmod 755 "$initdb/init.sh"

# The default image has no shell: mount a statically linked busybox as `/bin/sh`, as its documentation suggests.
shell_args=()
if ! docker run --rm --entrypoint /bin/sh "$image" -c true > /dev/null 2>&1; then
  shell_dir="$(mktemp -d)"
  busybox_cid="$(docker create busybox:musl)"
  docker cp "$busybox_cid:/bin/busybox" "$shell_dir/sh"
  docker rm -f "$busybox_cid" > /dev/null
  busybox_cid=""
  chmod 755 "$shell_dir" "$shell_dir/sh"
  shell_args=(-v "$shell_dir/sh":/bin/sh:ro)
fi

cid="$(
  docker run -d \
    "${shell_args[@]}" \
    -v "$users_d":/etc/clickhouse-server/users.d:ro \
    -v "$initdb":/docker-entrypoint-initdb.d:ro \
    --name "$(cname)" \
    "$image"
)"

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --password mounted_default_password \
    --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

chCli SHOW DATABASES | grep '^mounted_password_db$' > /dev/null
