#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SECRET_URL="http://leakuser:AVRO_LEAK_CANARY_9f3a2b@registry.invalid:8080/subjects"
PLAIN_URL="http://registry.invalid:8080/subjects"
SECRET_DISK="disk(type = 's3', endpoint = 'http://localhost:9000/x', access_key_id = 'AK_LEAK_CANARY', secret_access_key = 'SK_LEAK_CANARY')"

# A query_log entry is written after the response is sent, so wait for it to appear.
# An empty result after the last attempt is printed as is and fails the test.
read_query_log() {
    local result=""
    for _ in {1..100}; do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
        result=$($CLICKHOUSE_CLIENT -q "
            SELECT $1 FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '$2'
              AND type = 'QueryFinish' AND event_date >= yesterday()
            ORDER BY event_time_microseconds DESC LIMIT 1")
        [ -n "$result" ] && break
        sleep 0.3
    done
    echo "$result"
}

# The password is hidden while scheme, user, host, port and path survive.
echo 'query_log, URI password'
$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null SETTINGS log_comment = 'settings_map_mask_uri', format_avro_schema_registry_url = '$SECRET_URL'"
read_query_log "Settings['format_avro_schema_registry_url']" settings_map_mask_uri

# A value that carries no credential is passed through unchanged.
echo 'query_log, no credential'
$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null SETTINGS log_comment = 'settings_map_mask_plain', format_avro_schema_registry_url = '$PLAIN_URL'"
read_query_log "Settings['format_avro_schema_registry_url']" settings_map_mask_plain

echo 'processes, URI password'
$CLICKHOUSE_CLIENT -q "
    SELECT Settings['format_avro_schema_registry_url'] FROM system.processes
    WHERE query_id = queryID() SETTINGS format_avro_schema_registry_url = '$SECRET_URL'"

# A custom setting holding a disk definition is only reachable over an HTTP session:
# the native protocol ships it as a Field dump, which cannot be restored.
SESSION="05054_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION}&session_timeout=60" \
    --data-binary "SET SQL_05054_plain = 'no-secret-here', SQL_05054_disk = $SECRET_DISK"

echo 'processes, custom settings (plain, disk)'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION}" --data-binary "
    SELECT Settings['SQL_05054_plain'], Settings['SQL_05054_disk'] FROM system.processes
    WHERE query_id = queryID() SETTINGS log_comment = 'settings_map_mask_disk'"

echo 'query_log, custom settings (plain, disk)'
read_query_log "Settings['SQL_05054_plain'], Settings['SQL_05054_disk']" settings_map_mask_disk
