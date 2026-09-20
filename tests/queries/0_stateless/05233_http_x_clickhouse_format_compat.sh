#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `X-ClickHouse-Format` is an alias for `output_format` by default (since 26.8), and for `default_format`
# when `http_x_clickhouse_format_overrides_output_format` is disabled (the behavior of earlier versions).

URL_COMPAT="${CLICKHOUSE_URL}&http_x_clickhouse_format_overrides_output_format=0"

echo "-- default: the header overrides the FORMAT clause of the query"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x FORMAT CSV'
echo "-- default: the header applies when the query has no FORMAT clause"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x'
echo "-- default: the header overrides the output_format URL parameter"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format=CSV" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x'
echo "-- default: the response header reports the effective format"
${CLICKHOUSE_CURL} -sS -i "${CLICKHOUSE_URL}" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x FORMAT CSV' | grep -i '^X-ClickHouse-Format' | tr -d '\r'

echo "-- disabled: the header only sets default_format, so the FORMAT clause wins"
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x FORMAT CSV'
echo "-- disabled: the header applies when the query has no FORMAT clause"
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x'
echo "-- disabled: the header overrides the default_format URL parameter"
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}&default_format=CSV" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x'
echo "-- disabled: an explicit output_format URL parameter wins over the header"
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}&output_format=CSV" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x'
echo "-- disabled: the response header reports the effective format"
${CLICKHOUSE_CURL} -sS -i "${URL_COMPAT}" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x FORMAT CSV' | grep -i '^X-ClickHouse-Format' | tr -d '\r'
echo "-- disabled: works on a read-only GET request too"
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}&query=SELECT+1+AS+x+FORMAT+CSV" -H 'X-ClickHouse-Format: JSONEachRow'
echo "-- disabled: the header does not change the input format of an INSERT body"
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}" -d 'CREATE TABLE t_05233 (s String, n UInt8) ENGINE = Memory'
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}" -H 'X-ClickHouse-Format: JSONEachRow' -d 'INSERT INTO t_05233 FORMAT CSV
"a",1'
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}" -d 'SELECT * FROM t_05233 FORMAT JSONEachRow'
${CLICKHOUSE_CURL} -sS "${URL_COMPAT}" -d 'DROP TABLE t_05233'

echo "-- disabled via the session (as a profile would): the FORMAT clause wins"
SESSION_ID="${CLICKHOUSE_DATABASE}_05233"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -d 'SET http_x_clickhouse_format_overrides_output_format = 0'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID}" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x FORMAT CSV'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID}&close_session=1" -d 'SELECT 1 AS x FORMAT Null'

echo "-- disabled as a URL parameter in a readonly = 1 session: the setting is always changeable in read-only mode"
# A bare URL: the randomized settings the harness puts into CLICKHOUSE_URL are not changeable under readonly = 1.
SESSION_ID_RO="${CLICKHOUSE_DATABASE}_05233_ro"
URL_RO="${CLICKHOUSE_URL%%\?*}?database=${CLICKHOUSE_DATABASE}&session_id=${SESSION_ID_RO}"
${CLICKHOUSE_CURL} -sS "${URL_RO}" -d 'SET readonly = 1'
${CLICKHOUSE_CURL} -sS "${URL_RO}&http_x_clickhouse_format_overrides_output_format=0" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x FORMAT CSV'
${CLICKHOUSE_CURL} -sS "${URL_RO}&http_x_clickhouse_format_overrides_output_format=1" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x FORMAT CSV'
echo "-- ... while an ordinary setting is rejected in the same session"
${CLICKHOUSE_CURL} -sS "${URL_RO}&max_rows_to_read=1" -d 'SELECT 1 AS x FORMAT CSV' | grep -o "Cannot modify 'max_rows_to_read' setting in readonly mode"
${CLICKHOUSE_CURL} -sS "${URL_RO}&close_session=1" -d 'SELECT 1 AS x FORMAT Null'

echo "-- compatibility with a version before 26.8 restores the old header behavior"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&compatibility=26.7" -H 'X-ClickHouse-Format: JSONEachRow' -d 'SELECT 1 AS x FORMAT CSV'
