#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function kill_query()
{
    local query_id="$1"
    local user_name="$2"

    local user_name_parameter=""
    if [[ -n "$user_name" ]]; then
        user_name_parameter="--user $user_name"
    fi

    $CLICKHOUSE_CLIENT $user_name_parameter --query "KILL QUERY WHERE query_id = '$query_id' AND current_database = currentDatabase()" >/dev/null

    while [[ $($CLICKHOUSE_CLIENT --query="SELECT count() FROM system.processes WHERE query_id = '$query_id'") != 0 ]]; do sleep 0.1; done
}

query_id_1="02932_query_id_1_${CLICKHOUSE_DATABASE}_$RANDOM"
query_id_2="02932_query_id_2_${CLICKHOUSE_DATABASE}_$RANDOM"
query_id_3="02932_query_id_3_${CLICKHOUSE_DATABASE}_$RANDOM"
query_id_4="02932_query_id_4_${CLICKHOUSE_DATABASE}_$RANDOM"

unprivileged_user="test_user_02932_${CLICKHOUSE_DATABASE}_$RANDOM"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $unprivileged_user"
$CLICKHOUSE_CLIENT --query "CREATE USER $unprivileged_user"

long_query="SELECT sleep(1) FROM numbers(10) SETTINGS max_block_size=1;"

# A privileged user can kill any queries.
$CLICKHOUSE_CLIENT --query_id="$query_id_1" --query "$long_query" >/dev/null 2>&1 &
echo "Cancelling query 1"
kill_query "$query_id_1"

$CLICKHOUSE_CLIENT --query_id="$query_id_2" --user "$unprivileged_user" --query "$long_query" >/dev/null 2>&1 &
echo "Cancelling query 2"
kill_query "$query_id_2"

# Unprivileged user can kill only his own queries.
$CLICKHOUSE_CLIENT --query_id="$query_id_3" --user "$unprivileged_user" --query "$long_query" >/dev/null 2>&1 &
echo "Cancelling query 3"
kill_query "$query_id_3" "$unprivileged_user"

$CLICKHOUSE_CLIENT --query_id="$query_id_4" --query "$long_query" >/dev/null 2>&1 &
echo "Cancelling query 4"
kill_query "$query_id_4" "$unprivileged_user"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS;"

$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id = '$query_id_1'" | grep -oF "QUERY_WAS_CANCELLED"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id = '$query_id_2'" | grep -oF "QUERY_WAS_CANCELLED"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id = '$query_id_3'" | grep -oF "QUERY_WAS_CANCELLED"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id = '$query_id_4'" | grep -oF "ACCESS_DENIED"

$CLICKHOUSE_CLIENT --query "DROP USER $unprivileged_user"
