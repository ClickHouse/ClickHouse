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

# 5. A value of any other type reaches a setting too: an Array or a Tuple only as a typed query
# parameter, a `disk(...)` AST in SQL, and the raw text of a number literal in the AST JSON dialect.
# Each is hidden the same way, and the error that rejects it names only the type of the value.
check() {
    LOGGED=$($CLICKHOUSE_CLIENT --send_logs_level=error "$@" 2>&1 | grep -F '<Error> executeQuery')
    echo "$LOGGED" | grep -oE "in query: [^)]*" | head -1
    echo "$LOGGED" | grep -oE "to value of type [A-Za-z]+" | head -1
    echo "$LOGGED" | grep -o "$MAP_CANARY" | wc -l
}
check --param_p="{'u':'u:$MAP_CANARY@h'}" -q "SET url_base = {p:Map(String, String)}"
check --param_p="['u:$MAP_CANARY@h']" -q "SET url_base = {p:Array(String)}"
check --param_p="('u', 'u:$MAP_CANARY@h')" -q "SET url_base = {p:Tuple(String, String)}"
check -q "SET url_base = disk(type = 'local', path = '/$MAP_CANARY/')"
check --dialect clickhouse_json --enable_json_ast_dialect 1 -q \
    "{\"type\":\"SetQuery\",\"is_standalone\":true,\"changes\":[{\"name\":\"url_base\",\"value\":{\"field_type\":\"Number\",\"value\":\"Number_u:$MAP_CANARY@h\"}}]}"
