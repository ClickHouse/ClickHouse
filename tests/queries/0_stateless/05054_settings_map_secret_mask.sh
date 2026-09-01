#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SECRET_URL="http://leakuser:AVRO_LEAK_CANARY_9f3a2b@registry.invalid:8080/subjects"
PLAIN_URL="http://registry.invalid:8080/subjects"
SECRET_DISK="disk(type = 's3', endpoint = 'http://localhost:9000/x', access_key_id = 'AK_LEAK_CANARY', secret_access_key = 'SK_LEAK_CANARY')"
SECRET_BASE="http://baseuser:URL_BASE_LEAK_CANARY_4c1e7d@base.invalid/dir/"
SECRET_CUSTOM="http://customuser:CUSTOM_URI_LEAK_CANARY_7b2e91@custom.invalid/p"
PRESIGNED="https://bucket.s3.amazonaws.com/k?X-Amz-Credential=PRESIGN_ID_CANARY_1a2b&X-Amz-Signature=PRESIGN_SIG_CANARY_9d4e&list-type=2"
PLAIN_QUERY="https://bucket.s3.amazonaws.com/k?list-type=2&prefix=a/b"
TEXT_AFTER_URL="https://b/k?X-Amz-Signature=FREE_TAIL_CANARY_6a4c and then some trailing words"
TEXT_BEFORE_URL="note=https://b/k?X-Amz-Signature=FREE_HEAD_CANARY_7d5b"
CREDENTIAL_THEN_COMMA="https://b/k?X-Amz-Signature=COMMA_TAIL_CANARY_3c8f,request=42"
PRESIGNED_LAST="https://bucket.s3.amazonaws.com/k?list-type=2&X-Amz-Signature=PRESIGN_TAIL_CANARY_3f81"
PRESIGNED_NESTED="https://bucket.s3.amazonaws.com/n?list-type=2&X-Amz-Signature=PRESIGN_NEST_CANARY_2b7a"
PRESIGNED_MAP="https://bucket/k?X-Amz-Signature=PRESIGN_MAP_CANARY_5c7d"

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
    --data-binary "SET SQL_05054_plain = 'no-secret-here', SQL_05054_disk = $SECRET_DISK, SQL_05054_uri = '$SECRET_CUSTOM'"

echo 'processes, custom settings (plain, disk)'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION}" --data-binary "
    SELECT Settings['SQL_05054_plain'], Settings['SQL_05054_disk'] FROM system.processes
    WHERE query_id = queryID() SETTINGS log_comment = 'settings_map_mask_disk'"

echo 'query_log, custom settings (plain, disk)'
read_query_log "Settings['SQL_05054_plain'], Settings['SQL_05054_disk']" settings_map_mask_disk

# url_base is DECLARE(String, ...), so it is only reached by masking on the value shape.
echo 'query_log, String setting with a URI password'
$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null SETTINGS log_comment = 'settings_map_mask_url_base', url_base = '$SECRET_BASE'"
read_query_log "Settings['url_base']" settings_map_mask_url_base

echo 'processes, String setting with a URI password'
$CLICKHOUSE_CLIENT -q "
    SELECT Settings['url_base'] FROM system.processes
    WHERE query_id = queryID() SETTINGS url_base = '$SECRET_BASE'"

# A custom setting holding a plain String is not a secret CustomType, so only its value shape reaches it.
echo 'processes, custom setting with a URI password'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION}" --data-binary "
    SELECT Settings['SQL_05054_uri'] FROM system.processes
    WHERE query_id = queryID() SETTINGS log_comment = 'settings_map_mask_custom_uri'"

echo 'query_log, custom setting with a URI password'
read_query_log "Settings['SQL_05054_uri']" settings_map_mask_custom_uri

# A presigned URL carries its credential in query parameters instead of the userinfo.
echo 'query_log, presigned URL parameters'
$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null SETTINGS log_comment = 'settings_map_mask_presigned', s3_base = '$PRESIGNED'"
read_query_log "Settings['s3_base']" settings_map_mask_presigned

echo 'processes, presigned URL parameters'
$CLICKHOUSE_CLIENT -q "
    SELECT Settings['s3_base'] FROM system.processes
    WHERE query_id = queryID() SETTINGS s3_base = '$PRESIGNED'"

# A presigned parameter value runs to the end of the text, so free text is left alone. One arm per
# half of that precondition, so removing either half reddens exactly one of them.
echo 'processes, free text after the URL'
$CLICKHOUSE_CLIENT -q "
    SELECT Settings['log_comment'] FROM system.processes
    WHERE query_id = queryID() SETTINGS log_comment = '$TEXT_AFTER_URL'"

echo 'processes, free text before the URL'
$CLICKHOUSE_CLIENT -q "
    SELECT Settings['log_comment'] FROM system.processes
    WHERE query_id = queryID() SETTINGS log_comment = '$TEXT_BEFORE_URL'"

# A custom setting is shown as a Field dump, so the quotes and brackets around a masked leaf have to survive.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION}" --data-binary \
    "SET SQL_05054_tail = '$PRESIGNED_LAST', SQL_05054_nested = {'endpoint':'$PRESIGNED_NESTED','region':'eu-west-1'}"

echo 'processes, custom setting ending in a credential'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION}" --data-binary "
    SELECT Settings['SQL_05054_tail'] FROM system.processes WHERE query_id = queryID()"

echo 'processes, custom setting with a nested credential'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION}" --data-binary "
    SELECT Settings['SQL_05054_nested'] FROM system.processes WHERE query_id = queryID()"

# A Map setting serializes to more than one value, so the entry after the credential has to survive.
echo 'processes, Map setting with presigned URL parameters'
$CLICKHOUSE_CLIENT -q "
    SELECT Settings['http_response_headers'] FROM system.processes WHERE query_id = queryID()
    SETTINGS http_response_headers = {'Location':'$PRESIGNED_MAP','X-Kept':'yes'}"

# Query parameters that name no credential are passed through unchanged.
echo 'processes, query parameters that are not credentials'
$CLICKHOUSE_CLIENT -q "
    SELECT Settings['s3_base'] FROM system.processes
    WHERE query_id = queryID() SETTINGS s3_base = '$PLAIN_QUERY'"

# A parameter value ends at the next '&' or '#', so a comma belongs to the credential it follows.
echo 'processes, a credential followed by a comma'
$CLICKHOUSE_CLIENT -q "
    SELECT Settings['log_comment'] FROM system.processes
    WHERE query_id = queryID() SETTINGS log_comment = '$CREDENTIAL_THEN_COMMA'"
