#!/bin/bash
# Verify that the init scripts run only once when the server arguments select the data path (`-- --path=...`):
# `clickhouse docker-init` must check for an existing database in the path the server uses, not in the one of the
# configuration file, or it initializes again on every restart. `entrypoint.sh` of the Ubuntu image reads the path
# from the configuration file only, so the test runs only on the `FROM scratch` image.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

initdb="$(mktemp -d)"
cid=
trap '[ $? -eq 0 ] || [ -z "$cid" ] || dumpServerLogs "$cid"; [ -z "$cid" ] || docker rm -vf "$cid" > /dev/null; rm -rf "$initdb"' EXIT
cat > "$initdb/init.sql" <<'SQL'
CREATE TABLE IF NOT EXISTS default.bootstrap_rows (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO default.bootstrap_rows VALUES (1);
SQL
chmod 755 "$initdb"
chmod 644 "$initdb/init.sql"

# An anonymous volume at the data path keeps the data across restarts of the same container.
cid="$(
  docker run -d \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -v /mnt/clickhouse \
    -v "$initdb":/docker-entrypoint-initdb.d:ro \
    --name "$(cname)" \
    "$image" \
    -- --path=/mnt/clickhouse/
)"

rowsAre1() {
  [ "$(docker exec "$cid" clickhouse-client --query 'SELECT count() FROM default.bootstrap_rows')" = 1 ]
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  rowsAre1

# The database is in the data path from the server arguments.
pathExists "$cid" /mnt/clickhouse/data

docker restart "$cid" > /dev/null

# `docker logs` keeps the output of both starts. Wait until the second start has decided about the initialization:
# it either skips it or runs the init script a second time.
initRuns() {
  # The path is printed quoted (`std::filesystem::path`).
  docker logs "$cid" 2>&1 | grep -c -E 'docker-init: running "?/docker-entrypoint-initdb\.d/init\.sql' || true
}
initDecided() {
  docker logs "$cid" 2>&1 | grep -F 'skipping initialization' > /dev/null || [ "$(initRuns)" -ge 2 ]
}
# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  initDecided

if [ "$(initRuns)" != 1 ]; then
  echo "the init script ran again after the restart: the data path from the server arguments was not checked" >&2
  exit 1
fi

# The row count stays 1 once the server is up again.
# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  rowsAre1
