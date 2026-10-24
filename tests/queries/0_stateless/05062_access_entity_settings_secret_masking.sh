#!/usr/bin/env bash

# A credential embedded in the value of a setting assigned to a user, a role, or a settings profile
# must not reach `system.query_log.query` verbatim. Each case below carries its own canary string, so
# a leak points straight at the statement that leaked it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="u05062_$CLICKHOUSE_DATABASE"
ROLE_NAME="r05062_$CLICKHOUSE_DATABASE"
PROFILE_NAME="p05062_$CLICKHOUSE_DATABASE"

CREATE_USER_CANARY="c05062createuser"
ALTER_USER_CANARY="c05062alteruser"
CREATE_ROLE_CANARY="c05062createrole"
CREATE_PROFILE_CANARY="c05062createprofile"
ALTER_PROFILE_CANARY="c05062alterprofile"

check_query_log()
{
    local query_id="$1"
    local canary="$2"
    local logged
    for _ in {1..60}; do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
        logged=$($CLICKHOUSE_CLIENT -q "SELECT
                position(query, '[HIDDEN]') > 0,
                position(query, '$canary') = 0
            FROM system.query_log
            WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type != 'QueryStart'")
        [ -n "$logged" ] && break
        sleep 0.5
    done
    echo "$logged"
}

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $USER_NAME"
$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS $ROLE_NAME"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE_NAME"

# 1. `CREATE USER ... SETTINGS`.
QUERY_ID="05062_create_user_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$QUERY_ID&log_queries=1" \
    --data-binary "CREATE USER $USER_NAME SETTINGS format_avro_schema_registry_url = 'http://u:$CREATE_USER_CANARY@reg:8080/'" > /dev/null
check_query_log "$QUERY_ID" "$CREATE_USER_CANARY"

# 2. `ALTER USER ... SETTINGS`.
QUERY_ID="05062_alter_user_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$QUERY_ID&log_queries=1" \
    --data-binary "ALTER USER $USER_NAME SETTINGS format_avro_schema_registry_url = 'http://u:$ALTER_USER_CANARY@reg:8080/'" > /dev/null
check_query_log "$QUERY_ID" "$ALTER_USER_CANARY"

# 3. `CREATE ROLE ... SETTINGS`.
QUERY_ID="05062_create_role_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$QUERY_ID&log_queries=1" \
    --data-binary "CREATE ROLE $ROLE_NAME SETTINGS format_avro_schema_registry_url = 'http://u:$CREATE_ROLE_CANARY@reg:8080/'" > /dev/null
check_query_log "$QUERY_ID" "$CREATE_ROLE_CANARY"

# 4. `CREATE SETTINGS PROFILE ... SETTINGS`.
QUERY_ID="05062_create_profile_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$QUERY_ID&log_queries=1" \
    --data-binary "CREATE SETTINGS PROFILE $PROFILE_NAME SETTINGS format_avro_schema_registry_url = 'http://u:$CREATE_PROFILE_CANARY@reg:8080/'" > /dev/null
check_query_log "$QUERY_ID" "$CREATE_PROFILE_CANARY"

# 5. `ALTER SETTINGS PROFILE ... MODIFY SETTINGS`.
QUERY_ID="05062_alter_profile_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=$QUERY_ID&log_queries=1" \
    --data-binary "ALTER SETTINGS PROFILE $PROFILE_NAME MODIFY SETTINGS format_avro_schema_registry_url = 'http://u:$ALTER_PROFILE_CANARY@reg:8080/'" > /dev/null
check_query_log "$QUERY_ID" "$ALTER_PROFILE_CANARY"

$CLICKHOUSE_CLIENT -q "DROP USER $USER_NAME"
$CLICKHOUSE_CLIENT -q "DROP ROLE $ROLE_NAME"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE_NAME"
