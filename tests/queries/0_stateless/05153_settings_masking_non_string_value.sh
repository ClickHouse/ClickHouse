#!/usr/bin/env bash

# Masking a setting value must never throw. `nats_url`, `rabbitmq_address` and
# `after_processing_move_connection_string` hold a URL or a connection string whose credential is
# masked in place, and masking runs before the setting is validated, so the value can be of any
# type. Writing the port where the URL goes used to throw `BAD_GET` out of the masking, and the
# server then logged the statement text raw, with every other credential in it in cleartext.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CANARY="c05153natspassword"

# 1. A value of the wrong type reports the setting's own error, not one from the masking.
for setting in nats_url rabbitmq_address after_processing_move_connection_string; do
    $CLICKHOUSE_CLIENT -q "SET $setting = 4222" 2>&1 | grep -oE '\(UNKNOWN_SETTING\)|\(BAD_GET\)' | head -1
done

# 2. The credential next to the wrongly typed setting is still masked in what the server logs.
# Only the server's own log line is checked: the `(query: ...)` line next to it is the client
# echoing back what it sent, which is the text the user typed and is never masked.
$CLICKHOUSE_CLIENT --send_logs_level=error -q "SET nats_password = '$CANARY', nats_url = 4222" 2>&1 |
    grep -oE "in query: [^)]*" | head -1

# 3. And in `system.query_log`, for the `SETTINGS` clause of a `CREATE TABLE` as well.
QUERY_ID="05153_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" --log_queries=1 -q \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}.events (a String) ENGINE = MergeTree ORDER BY a
        SETTINGS nats_password = '$CANARY', nats_url = 4222" > /dev/null 2>&1

for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    LOGGED=$($CLICKHOUSE_CLIENT -q "SELECT
            position(query, '$CANARY') = 0,
            position(query, 'nats_password = \'[HIDDEN]\'') > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'ExceptionBeforeStart'")
    [ -n "$LOGGED" ] && break
    sleep 0.5
done
echo "$LOGGED"

# 4. Only a String can be searched for the credential, and a Map can carry one, so a value of another
# type is hidden whole, in the statement and in the error that rejects it.
MAP_CANARY="c05153mapvalue"
for setting in nats_url rabbitmq_address after_processing_move_connection_string \
    format_avro_schema_registry_url url_base s3_base; do
    LOGGED=$($CLICKHOUSE_CLIENT --send_logs_level=error -q "SET $setting = {'u':'u:$MAP_CANARY@h'}" 2>&1 |
        grep -F '<Error> executeQuery')
    echo "$LOGGED" | grep -oE "in query: [^)]*" | head -1
    echo "$LOGGED" | grep -o "$MAP_CANARY" | wc -l
done

# 5. An Array or a Tuple reaches a setting only as a typed query parameter, and so can a Map. Each is
# hidden the same way, and the error that rejects it names only the type of the value.
check_param() {
    LOGGED=$($CLICKHOUSE_CLIENT --send_logs_level=error --param_p="$2" -q "SET url_base = {p:$1}" 2>&1 |
        grep -F '<Error> executeQuery')
    echo "$LOGGED" | grep -oE "in query: [^)]*" | head -1
    echo "$LOGGED" | grep -oE "to value of type [A-Za-z]+" | head -1
    echo "$LOGGED" | grep -o "$MAP_CANARY" | wc -l
}
check_param "Map(String, String)" "{'u':'u:$MAP_CANARY@h'}"
check_param "Array(String)" "['u:$MAP_CANARY@h']"
check_param "Tuple(String, String)" "('u', 'u:$MAP_CANARY@h')"
