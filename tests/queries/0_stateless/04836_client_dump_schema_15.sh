#!/usr/bin/env bash

# `--dump-schema` probes the SELECT of a materialized view with `EXPLAIN QUERY TREE` on the source server, and analysis
# builds the storage of every table function in it. A static structure does not make that passive: `url` with a `*` in
# its path lists HTTP index pages under `allow_url_wildcard_from_index_pages`, and `file` lists the files of
# `user_files_path`. Such a view is not probed and keeps the gates its clauses may need; a view over a `url` that names
# one file is still probed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

DB="${CLICKHOUSE_DATABASE}"
# Nothing listens on port 1, and one try keeps a request that is sent from being retried.
WILDCARD_URL="http://127.0.0.1:1/${CLICKHOUSE_TEST_UNIQUE_NAME}_wildcard/*"
PLAIN_URL="http://127.0.0.1:1/${CLICKHOUSE_TEST_UNIQUE_NAME}_plain/data.csv"
FILE_GLOB="${CLICKHOUSE_TEST_UNIQUE_NAME}_file/**"

# Creating a view over the wildcard lists it too, and the failed listing is a warning.
$CLICKHOUSE_CLIENT --send_logs_level=error -n -q "
    CREATE TABLE ${DB}.src (x UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW ${DB}.mv_wildcard ENGINE = Memory AS
        SELECT s.x AS x FROM ${DB}.src AS s INNER JOIN url('${WILDCARD_URL}', CSV, 'v UInt64') AS u ON s.x = u.v
        SETTINGS allow_url_wildcard_from_index_pages = 1, http_max_tries = 1;
    CREATE MATERIALIZED VIEW ${DB}.mv_wildcard_group ENGINE = Memory AS
        SELECT u.v AS v, count() AS c FROM ${DB}.src AS s INNER JOIN url('${WILDCARD_URL}', CSV, 'v UInt64') AS u ON s.x = u.v
        GROUP BY v SETTINGS allow_url_wildcard_from_index_pages = 1, http_max_tries = 1;
    CREATE MATERIALIZED VIEW ${DB}.mv_file ENGINE = Memory AS
        SELECT s.x AS x FROM ${DB}.src AS s INNER JOIN file('${FILE_GLOB}', CSV, 'v UInt64') AS f ON s.x = f.v;
    CREATE MATERIALIZED VIEW ${DB}.mv_plain ENGINE = Memory AS
        SELECT s.x AS x FROM ${DB}.src AS s INNER JOIN url('${PLAIN_URL}', CSV, 'v UInt64') AS u ON s.x = u.v GROUP BY x;"

$CLICKHOUSE_CLIENT --dump-schema "$DB" > "$work/dump.sql" 2> "$work/err.txt"
rc=$?
echo "dump rc=$rc"
[ $rc -eq 0 ] || grep -o 'Code: [0-9]*' "$work/err.txt" | head -1

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'probes of the wildcard url: ' || toString(countIf(query LIKE '%_wildcard/*%'))
        || ', of the file glob: ' || toString(countIf(query LIKE '%_file/**%'))
        || ', of the plain url: ' || toString(countIf(query LIKE '%_plain/data.csv%') > 0)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type != 'QueryStart'
        AND query LIKE 'EXPLAIN QUERY TREE%'"

# Without a probe the gates stay on: `mv_wildcard_group` groups, and every unprobed view may need the bad-select gate.
echo "bad-select gate emitted: $(grep -c '^SET allow_materialized_view_with_bad_select = 1;' "$work/dump.sql")"
echo "GROUP BY gate emitted: $(grep -c '^SET allow_suspicious_types_in_group_by = 1;' "$work/dump.sql")"

rm -rf "$work/dst"
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/dump.sql" > /dev/null 2> "$work/err.txt"
rc=$?
echo "replay rc=$rc"
[ $rc -eq 0 ] || grep -o 'Code: [0-9]*' "$work/err.txt" | head -1
$CLICKHOUSE_LOCAL --path "$work/dst" -q "
    SELECT 'replayed views: ' || toString(count()) FROM system.tables WHERE database = '${DB}' AND name LIKE 'mv%'"
