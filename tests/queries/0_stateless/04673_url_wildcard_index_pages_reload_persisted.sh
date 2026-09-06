#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Two `clickhouse local` invocations against the same --path: the second reloads the metadata the
# first persisted, which is the path a server takes at startup. The host is unreachable on purpose:
# no arm needs a live server.
WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"

GLOB_URL="http://127.0.0.1:1/**/"

# The arms that get past the gate reach the unreachable host, where the default 10 attempts with
# backoff would cost minutes. One attempt reports the same refusal.
NO_RETRY="SET http_max_tries = 1;"

# Prints the tables the reload produced, then whether the experimental gate refused anything.
# Counting the gate rather than echoing the error keeps the reference free of connection wording.
run_local() {
    local out
    out=$(${CLICKHOUSE_LOCAL} --path="$1" --query "${NO_RETRY} $2" \
        -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0 2>&1)
    echo "$out" | grep -E '^(created|reloaded|plan)\b' || true
    echo -n 'gate refused: '
    echo "$out" | grep -c 'SUPPORT_IS_DISABLED' || true
}

# Reports whether the query itself succeeded, so an arm can assert a positive outcome rather than
# the absence of one error. `insert ok: 0` covers both a gate refusal and a read-only storage.
run_local_status() {
    if ${CLICKHOUSE_LOCAL} --path="$1" --query "${NO_RETRY} $2" \
        -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0 >/dev/null 2>&1
    then echo 'insert ok: 1'
    else echo 'insert ok: 0'
    fi
}

show_table() {
    echo "SELECT 'reloaded', engine FROM system.tables WHERE database = 'd' AND name = '$1';"
}

# Both candidate storages report engine `URL` in system.tables, so the engine name cannot say which
# one a reload picked. The read step names them apart: ReadFromURL is the plain storage,
# ReadFromObjectStorage the index-page-listing one. EXPLAIN stops before the pipeline, so no request
# is issued.
show_table_and_plan() {
    echo "$(show_table "$1")
          SELECT 'plan', countIf(explain ILIKE '%ReadFromURL%') > 0,
                         countIf(explain ILIKE '%ReadFromObjectStorage%') = 0
          FROM (EXPLAIN SELECT count() FROM d.$1);"
}

echo '--- A: explicit structure, created with the setting on, reloaded with it off'
D="${WORKING_FOLDER}/a"; mkdir -p "$D"
run_local "$D" "
    SET allow_experimental_url_wildcard_from_index_pages = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (x Int32) AS url('${GLOB_URL}', JSONEachRow, 'x Int32');
    SELECT 'created', engine FROM system.tables WHERE database = 'd' AND name = 't';"
run_local "$D" "$(show_table_and_plan t)"
run_local "$D" "SELECT count() FROM d.t;"

echo '--- A2: structure inferred, same cycle'
# LineAsString has a fixed schema, so reading only has to build the storage. A format that infers
# would instead spend the whole schema-inference retry budget against the unreachable host.
D="${WORKING_FOLDER}/a2"; mkdir -p "$D"
run_local "$D" "
    SET allow_experimental_url_wildcard_from_index_pages = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (line String) AS url('${GLOB_URL}', LineAsString);
    SELECT 'created', engine FROM system.tables WHERE database = 'd' AND name = 't';"
run_local "$D" "$(show_table_and_plan t)"
run_local "$D" "SELECT count() FROM d.t;"

echo '--- C: ENGINE = URL keeps its own behaviour'
D="${WORKING_FOLDER}/c"; mkdir -p "$D"
run_local "$D" "
    SET allow_experimental_url_wildcard_from_index_pages = 1;
    CREATE DATABASE d;
    CREATE TABLE d.u (x Int32) ENGINE = URL('${GLOB_URL}', 'TSV');
    SELECT 'created', engine FROM system.tables WHERE database = 'd' AND name = 'u';"
run_local "$D" "$(show_table u)"
echo 'creating one with the setting off'
D="${WORKING_FOLDER}/c2"; mkdir -p "$D"
run_local "$D" "
    CREATE DATABASE d;
    CREATE TABLE d.u (x Int32) ENGINE = URL('${GLOB_URL}', 'TSV');"

echo '--- D: a path without a listable glob never consults the setting'
D="${WORKING_FOLDER}/d"; mkdir -p "$D"
run_local "$D" "
    CREATE DATABASE d;
    CREATE TABLE d.t (x Int32) AS url('http://127.0.0.1:1/data', JSONEachRow, 'x Int32');
    SELECT 'created', engine FROM system.tables WHERE database = 'd' AND name = 't';"
run_local "$D" "$(show_table t)"

echo '--- E: INSERT INTO FUNCTION reaches a writable storage'
# A zero-row insert builds the write storage and commits without opening a connection, so a clean
# exit says the writable storage was selected: the read-only branch refuses a glob path outright.
D="${WORKING_FOLDER}/e"; mkdir -p "$D"
run_local_status "$D" "
    SET allow_experimental_url_wildcard_from_index_pages = 1;
    INSERT INTO FUNCTION url('${GLOB_URL}', JSONEachRow, 'x Int32') SELECT * FROM numbers(0);"
run_local_status "$D" "INSERT INTO FUNCTION url('${GLOB_URL}', JSONEachRow, 'x Int32') SELECT * FROM numbers(0);"
# One row still has to reach the host, which proves the arm above is not passing by short-circuit.
run_local "$D" "INSERT INTO FUNCTION url('${GLOB_URL}', JSONEachRow, 'x Int32') VALUES (1);"

echo '--- F: a scheme handled by a delegate is untouched'
D="${WORKING_FOLDER}/f"; mkdir -p "$D"
run_local "$D" "
    CREATE DATABASE d;
    CREATE TABLE d.t (x Int32) AS url('file:///nonexistent/x*.json', JSONEachRow, 'x Int32');
    SELECT 'created', engine FROM system.tables WHERE database = 'd' AND name = 't';"
run_local "$D" "$(show_table t)"

echo '--- G: a query against url() still requires the setting'
D="${WORKING_FOLDER}/g"; mkdir -p "$D"
run_local "$D" "SELECT * FROM url('${GLOB_URL}', JSONEachRow, 'x Int32');"
run_local "$D" "
    SET allow_experimental_url_wildcard_from_index_pages = 1;
    SELECT * FROM url('${GLOB_URL}', JSONEachRow, 'x Int32');"

rm -rf "${WORKING_FOLDER}"
