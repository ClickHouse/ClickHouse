#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a replica would recreate the views over a `URL` database that only this server has, and the
# dictionary is detached and attached again.

# `--dump-schema` probes the SELECT of a materialized view with `EXPLAIN QUERY TREE` on the source server. Such a view is
# probed only when analyzing it stays inside the server, otherwise it keeps the gates its clauses may need:
# - analysis builds the storage of every table function, and a static structure does not make that passive: `url` with a
#   `*` in its path lists HTTP index pages under `allow_url_wildcard_from_index_pages`, and `file` lists `user_files_path`;
# - it resolves every table, and a table of an external database, such as a `URL` one, is fetched from its source;
# - it folds a function with constant arguments, and `dictGet` then loads the dictionary from its source.
# A view over a `url` that names one file, or over tables that analyze in memory, is still probed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

DB="${CLICKHOUSE_DATABASE}"
URL_DB="${CLICKHOUSE_DATABASE}_url"
work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"; $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${URL_DB}"' EXIT

# Nothing listens on port 1, and one try keeps a request that is sent from being retried.
WILDCARD_URL="http://127.0.0.1:1/${CLICKHOUSE_TEST_UNIQUE_NAME}_wildcard/*"
PLAIN_URL="http://127.0.0.1:1/${CLICKHOUSE_TEST_UNIQUE_NAME}_plain/data.csv"
FILE_GLOB="${CLICKHOUSE_TEST_UNIQUE_NAME}_file/**"
# The `URL` database and the dictionary read this server over HTTP, so `system.query_log` shows every request they send.
URL_DB_DEFINITION="CREATE DATABASE ${URL_DB} ENGINE = URL('http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/')"
URL_MARKER="${DB}_url_marker"
DICT_MARKER="${DB}_dict_marker"
URL_TABLE="\`?query=SELECT%2042%20AS%20x%2C%20'${URL_MARKER}'%20AS%20m%20FORMAT%20CSVWithNames\`"

# Creating a view over the wildcard lists it too, and the failed listing is a warning.
$CLICKHOUSE_CLIENT --send_logs_level=error -n -q "
    ${URL_DB_DEFINITION};
    CREATE TABLE ${DB}.src (x UInt64) ENGINE = Memory;
    CREATE DICTIONARY ${DB}.http_dict (k UInt64, v String) PRIMARY KEY k
        SOURCE(HTTP(URL '${CLICKHOUSE_URL}&query=SELECT%201%2C%20%27${DICT_MARKER}%27%20FORMAT%20TabSeparated' FORMAT 'TabSeparated'))
        LAYOUT(FLAT()) LIFETIME(0);
    CREATE MATERIALIZED VIEW ${DB}.mv_wildcard ENGINE = Memory AS
        SELECT s.x AS x FROM ${DB}.src AS s INNER JOIN url('${WILDCARD_URL}', CSV, 'v UInt64') AS u ON s.x = u.v
        SETTINGS allow_url_wildcard_from_index_pages = 1, http_max_tries = 1;
    CREATE MATERIALIZED VIEW ${DB}.mv_wildcard_group ENGINE = Memory AS
        SELECT u.v AS v, count() AS c FROM ${DB}.src AS s INNER JOIN url('${WILDCARD_URL}', CSV, 'v UInt64') AS u ON s.x = u.v
        GROUP BY v SETTINGS allow_url_wildcard_from_index_pages = 1, http_max_tries = 1;
    CREATE MATERIALIZED VIEW ${DB}.mv_file ENGINE = Memory AS
        SELECT s.x AS x FROM ${DB}.src AS s INNER JOIN file('${FILE_GLOB}', CSV, 'v UInt64') AS f ON s.x = f.v;
    CREATE MATERIALIZED VIEW ${DB}.mv_plain ENGINE = Memory AS
        SELECT s.x AS x FROM ${DB}.src AS s INNER JOIN url('${PLAIN_URL}', CSV, 'v UInt64') AS u ON s.x = u.v GROUP BY x;
    CREATE MATERIALIZED VIEW ${DB}.mv_url_database ENGINE = Memory AS
        SELECT s.x AS x, count() AS c FROM ${DB}.src AS s LEFT JOIN ${URL_DB}.${URL_TABLE} AS t ON s.x = t.x GROUP BY x;
    CREATE MATERIALIZED VIEW ${DB}.mv_dict_get ENGINE = Memory AS
        SELECT x, dictGet('${DB}.http_dict', 'v', toUInt64(1)) AS v FROM ${DB}.src;
    CREATE MATERIALIZED VIEW ${DB}.mv_passive ENGINE = Memory AS
        SELECT s.x AS x, count() AS c FROM ${DB}.src AS s INNER JOIN ${DB}.http_dict AS h ON s.x = h.k
        INNER JOIN system.one AS o ON s.x = o.dummy GROUP BY x;"
# Creating the views loaded the dictionary; attaching it again unloads it.
$CLICKHOUSE_CLIENT -q "DETACH DICTIONARY ${DB}.http_dict"
$CLICKHOUSE_CLIENT -q "ATTACH DICTIONARY ${DB}.http_dict"

# Prints the HTTP requests of the `URL` database and of the dictionary so far. A request is logged when it starts, before
# its answer reaches the reader.
http_requests()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT countIf(query LIKE '%${URL_MARKER}%'), countIf(query LIKE '%${DICT_MARKER}%') FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryStart' AND interface = 2" | tr '\t' ' '
}
read -r url_before dict_before <<< "$(http_requests)"

$CLICKHOUSE_CLIENT --dump-schema "$DB" > "$work/dump.sql" 2> "$work/err.txt"
rc=$?
echo "dump rc=$rc"
[ $rc -eq 0 ] || grep -o 'Code: [0-9]*' "$work/err.txt" | head -1

read -r url_after dict_after <<< "$(http_requests)"
echo "HTTP requests the dump sent for the URL database: $((url_after - url_before)), for the dictionary: $((dict_after - dict_before))"
echo "dictionary after the dump: $($CLICKHOUSE_CLIENT -q "SELECT status FROM system.dictionaries WHERE database = '${DB}' AND name = 'http_dict'")"

$CLICKHOUSE_CLIENT -q "
    SELECT 'probes of the wildcard url: ' || toString(countIf(query LIKE '%_wildcard/*%'))
        || ', of the file glob: ' || toString(countIf(query LIKE '%_file/**%'))
        || ', of the plain url: ' || toString(countIf(query LIKE '%_plain/data.csv%') > 0)
        || ', of the URL database: ' || toString(countIf(query LIKE '%${URL_MARKER}%'))
        || ', of the dictionary function: ' || toString(countIf(query LIKE '%dictGet(%'))
        || ', of the in-memory tables: ' || toString(countIf(query LIKE '%system.one AS o%') > 0)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type != 'QueryStart'
        AND query LIKE 'EXPLAIN QUERY TREE%'"

# Without a probe the gates stay on: the views group, and every unprobed view may need the bad-select gate.
echo "bad-select gate emitted: $(grep -c '^SET allow_materialized_view_with_bad_select = 1;' "$work/dump.sql")"
echo "GROUP BY gate emitted: $(grep -c '^SET allow_suspicious_types_in_group_by = 1;' "$work/dump.sql")"

# The dump does not create the `URL` database its view reads, so the target has it already.
echo "${URL_DB_DEFINITION};" > "$work/replay.sql"
cat "$work/dump.sql" >> "$work/replay.sql"
$CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/replay.sql" > /dev/null 2> "$work/err.txt"
rc=$?
echo "replay rc=$rc"
[ $rc -eq 0 ] || grep -o 'Code: [0-9]*' "$work/err.txt" | head -1
$CLICKHOUSE_LOCAL --path "$work/dst" -q "
    SELECT 'replayed views: ' || toString(count()) FROM system.tables WHERE database = '${DB}' AND name LIKE 'mv%'"
