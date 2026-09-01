#!/usr/bin/env bash

# A password embedded in the value of a setting must not reach any place that prints setting values.
# The test server does not enable `display_secrets_in_show_and_select`, so everything below is always
# masked. Each case uses its own canary string, so a leak points straight at the sink that leaked it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AVRO_CANARY="c05056avrolog"
URLBASE_CANARY="c05056urlbaselog"
SETTINGS_CANARY="c05056systemsettings"
PROCESSES_CANARY="c05056systemprocesses"
PROFILE_CANARY="c05056settingsprofile"

# 1. `system.query_log`: the query text, the formatted query text and the `Settings` map.
QUERY_ID="05056_query_log_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$QUERY_ID&log_queries=1&log_formatted_queries=1" \
    --data-binary "SELECT 1 SETTINGS format_avro_schema_registry_url = 'http://u:$AVRO_CANARY@reg:8080/', url_base = 'https://u:$URLBASE_CANARY@example.com/d/'"

for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    LOGGED=$($CLICKHOUSE_CLIENT -q "SELECT
            position(query, '[HIDDEN]') > 0,
            position(formatted_query, '[HIDDEN]') > 0,
            position(Settings['format_avro_schema_registry_url'], '[HIDDEN]') > 0,
            position(Settings['url_base'], '[HIDDEN]') > 0,
            position(concat(query, formatted_query, Settings['format_avro_schema_registry_url'], Settings['url_base']), '$AVRO_CANARY') = 0,
            position(concat(query, formatted_query, Settings['format_avro_schema_registry_url'], Settings['url_base']), '$URLBASE_CANARY') = 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'QueryFinish'")
    [ -n "$LOGGED" ] && break
    sleep 0.5
done
echo "$LOGGED"

# 2. `system.settings`.
$CLICKHOUSE_CLIENT -q "SELECT value FROM system.settings WHERE name IN ('format_avro_schema_registry_url', 'url_base') ORDER BY name
    SETTINGS format_avro_schema_registry_url = 'http://u:$SETTINGS_CANARY@reg:8080/', url_base = 'https://u:$SETTINGS_CANARY@example.com/d/'"

# 3. `system.processes`, read while the query that carries the secret is still running.
PROCESSES_QUERY_ID="05056_processes_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$PROCESSES_QUERY_ID" \
    --data-binary "SELECT count(sleepEachRow(1)) FROM system.numbers LIMIT 30
        SETTINGS max_block_size = 1, format_avro_schema_registry_url = 'http://u:$PROCESSES_CANARY@reg:8080/'" > /dev/null 2>&1 &

for _ in {1..60}; do
    RUNNING=$($CLICKHOUSE_CLIENT -q "SELECT
            position(Settings['format_avro_schema_registry_url'], '[HIDDEN]') > 0,
            position(Settings['format_avro_schema_registry_url'], '$PROCESSES_CANARY') = 0
        FROM system.processes WHERE query_id = '$PROCESSES_QUERY_ID'")
    [ -n "$RUNNING" ] && break
    sleep 0.3
done
echo "$RUNNING"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$PROCESSES_QUERY_ID' SYNC FORMAT Null"
wait

# 4. A settings profile: `SHOW CREATE SETTINGS PROFILE` and `system.settings_profile_elements`.
PROFILE="p05056_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $PROFILE SETTINGS format_avro_schema_registry_url = 'http://u:$PROFILE_CANARY@reg:8080/'"
$CLICKHOUSE_CLIENT -q "SHOW CREATE SETTINGS PROFILE $PROFILE" | sed "s/$PROFILE/profile/"
$CLICKHOUSE_CLIENT -q "SELECT value FROM system.settings_profile_elements WHERE profile_name = '$PROFILE'"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE"
