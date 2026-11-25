#!/bin/bash
# Verify that a `*.sql.gz` init script runs with a read-only root filesystem (`docker run --read-only`) and writable
# mounts for the data, logs, `/tmp`, and `users.d`: `clickhouse docker-init` decompresses it with `clickhouse local`,
# which must not create its persistent storage in the read-only home directory. `entrypoint.sh` of the Ubuntu image
# uses `gunzip`, so the test runs only on the `FROM scratch` image.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

initdb="$(mktemp -d)"
cid=
trap '[ $? -eq 0 ] || [ -z "$cid" ] || dumpServerLogs "$cid"; [ -z "$cid" ] || docker rm -vf "$cid" > /dev/null; rm -rf "$initdb"' EXIT
echo 'CREATE TABLE default.initdb_sql_gz (x UInt8) ENGINE = MergeTree ORDER BY x; INSERT INTO default.initdb_sql_gz VALUES (42);' \
  | gzip > "$initdb/init.sql.gz"
chmod 755 "$initdb"
chmod 644 "$initdb/init.sql.gz"

cid="$(
  docker run -d \
    --read-only \
    --tmpfs /tmp \
    --tmpfs /var/lib/clickhouse \
    --tmpfs /var/log/clickhouse-server \
    --tmpfs /etc/clickhouse-server/users.d \
    -v "$initdb":/docker-entrypoint-initdb.d:ro \
    --name "$(cname)" \
    "$image"
)"

# The table exists only if the init script ran.
checkInitialized() {
  [ "$(docker exec "$cid" clickhouse-client --query 'SELECT sum(x) FROM default.initdb_sql_gz')" = 42 ]
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  checkInitialized
