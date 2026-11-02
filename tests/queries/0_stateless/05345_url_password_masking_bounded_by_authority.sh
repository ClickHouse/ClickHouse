#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The password of a URL is hidden only inside its authority, which ends at the first '/', '?' or '#'.
$CLICKHOUSE_CLIENT <<'SQL'
-- A '@' in the query string is not the end of a password: the URL stays intact.
CREATE TABLE t_url_query (x UInt8) ENGINE = URL('http://localhost:11111/x?email=a@b.com', 'CSV');
SHOW CREATE TABLE t_url_query SETTINGS format_display_secrets_in_show_and_select = 0;

-- A raw '@' in the password is hidden up to the last '@' of the authority.
CREATE TABLE t_url_at (x UInt8) ENGINE = URL('https://user:SEKRIT@PW@localhost:11111/x', 'CSV');
SHOW CREATE TABLE t_url_at SETTINGS format_display_secrets_in_show_and_select = 0;

-- The same for the url of a dictionary source.
CREATE DICTIONARY d_url_query (k UInt64, v String) PRIMARY KEY k
SOURCE(HTTP(URL 'http://localhost:11111/x?email=a@b.com' FORMAT 'TSV')) LAYOUT(FLAT()) LIFETIME(0);
SHOW CREATE DICTIONARY d_url_query SETTINGS format_display_secrets_in_show_and_select = 0;

CREATE DICTIONARY d_url_at (k UInt64, v String) PRIMARY KEY k
SOURCE(HTTP(URL 'https://user:SEKRIT@PW@localhost:11111/x' FORMAT 'TSV')) LAYOUT(FLAT()) LIFETIME(0);
SHOW CREATE DICTIONARY d_url_at SETTINGS format_display_secrets_in_show_and_select = 0;
SQL

# A setting value can be any kind of URI, so its password is hidden up to the last '@' before the first '/'.
$CLICKHOUSE_CLIENT -q "SELECT 1 SETTINGS format_avro_schema_registry_url = 'mongodb://usr:999999?TOPSECRET@localhost:27017/db'" 2>&1 \
    | grep -o "while setting 'format_avro_schema_registry_url' to value '[^']*'"

# The same in the query text of `system.query_log`. Sent over HTTP, because the client rejects the value before sending it.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${CLICKHOUSE_DATABASE}_settings_in_query_log" \
    -d "SELECT 1 SETTINGS format_avro_schema_registry_url = 'mongodb://usr:999999?TOPSECRET@localhost:27017/db'" | grep -o 'BAD_ARGUMENTS'
# The entry is written after the HTTP response is sent, so wait for it.
for _ in {1..100}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    result=$($CLICKHOUSE_CLIENT -q "
        SELECT type, countIf(query LIKE '%TOPSECRET%'), countIf(query LIKE '%usr:[HIDDEN]@localhost%')
        FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '${CLICKHOUSE_DATABASE}_settings_in_query_log'
        GROUP BY type")
    [ -n "$result" ] && break
    sleep 0.1
done
echo "$result"
