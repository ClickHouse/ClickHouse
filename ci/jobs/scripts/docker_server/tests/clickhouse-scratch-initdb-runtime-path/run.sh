#!/bin/bash
# Verify that the init scripts run only once when the server arguments select the data path, directly
# (`-- --path=...`) or through a property the path refers to (`<path>${macros.data_path}</path>` with
# `-- --macros.data_path=...`): `clickhouse docker-init` must check for an existing database in the path the server
# uses, not in the one of the configuration file alone, or it initializes again on every restart. `entrypoint.sh` of
# the Ubuntu image reads the path from the configuration file only, so the test runs only on the `FROM scratch` image.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

initdb="$(mktemp -d)"
config="$(mktemp -d)"
cid=
trap '[ $? -eq 0 ] || [ -z "$cid" ] || dumpServerLogs "$cid"; [ -z "$cid" ] || docker rm -vf "$cid" > /dev/null; rm -rf "$initdb" "$config"' EXIT
cat > "$initdb/init.sql" <<'SQL'
CREATE TABLE IF NOT EXISTS default.bootstrap_rows (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO default.bootstrap_rows VALUES (1);
SQL
chmod 755 "$initdb"
chmod 644 "$initdb/init.sql"

cat > "$config/data-path-macro.xml" <<'XML'
<clickhouse>
    <path>${macros.data_path}</path>
    <macros>
        <data_path>/var/lib/clickhouse/</data_path>
    </macros>
</clickhouse>
XML
chmod -R a+rX "$config"

rowsAre1() {
  [ "$(docker exec "$cid" clickhouse-client --query 'SELECT count() FROM default.bootstrap_rows')" = 1 ]
}

# `docker logs` keeps the output of both starts.
initRuns() {
  # The path is printed quoted (`std::filesystem::path`).
  docker logs "$cid" 2>&1 | grep -c -E 'docker-init: running "?/docker-entrypoint-initdb\.d/init\.sql' || true
}
# Whether the second start has decided about the initialization: it either skips it or runs the init script again.
initDecided() {
  docker logs "$cid" 2>&1 | grep -F 'skipping initialization' > /dev/null || [ "$(initRuns)" -ge 2 ]
}

# Start with the docker arguments ($1) and the server arguments (the rest), with an anonymous volume at
# `/mnt/clickhouse`, which keeps the data across restarts of the same container; restart once.
checkInitializedOnce() {
  local docker_args="$1"
  shift
  # shellcheck disable=SC2086 # `docker_args` is a list of options.
  cid="$(
    docker run -d \
      -e CLICKHOUSE_SKIP_USER_SETUP=1 \
      -v /mnt/clickhouse \
      -v "$initdb":/docker-entrypoint-initdb.d:ro \
      $docker_args \
      --name "$(cname)" \
      "$image" "$@"
  )"

  # shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
  . "$TESTS_LIB_DIR/retry.sh" \
    --tries "$CLICKHOUSE_TEST_TRIES" \
    --sleep "$CLICKHOUSE_TEST_SLEEP" \
    rowsAre1

  # The database is in the data path from the server arguments.
  pathExists "$cid" /mnt/clickhouse/data

  docker restart "$cid" > /dev/null

  # shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
  . "$TESTS_LIB_DIR/retry.sh" \
    --tries "$CLICKHOUSE_TEST_TRIES" \
    --sleep "$CLICKHOUSE_TEST_SLEEP" \
    initDecided

  if [ "$(initRuns)" != 1 ]; then
    echo "the init script ran again after the restart: the data path the server uses was not checked" >&2
    exit 1
  fi

  # The row count stays 1 once the server is up again.
  # shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
  . "$TESTS_LIB_DIR/retry.sh" \
    --tries "$CLICKHOUSE_TEST_TRIES" \
    --sleep "$CLICKHOUSE_TEST_SLEEP" \
    rowsAre1

  docker rm -vf "$cid" > /dev/null
  cid=
}

# The path itself in the server arguments.
checkInitializedOnce "" -- --path=/mnt/clickhouse/

# A property the path refers to in the server arguments.
checkInitializedOnce "-v $config/data-path-macro.xml:/etc/clickhouse-server/config.d/data-path-macro.xml:ro" \
  -- --macros.data_path=/mnt/clickhouse/
