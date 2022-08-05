#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function get_dictionary_status()
{
    local name=$1 && shift
    $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL_PROMETHEUS" | {
        awk -F'[{}=," ]' -vname="$name" '/ClickHouseStatusInfo_DictionaryStatus{/ && $(NF-3) == name { print $4, $NF }'
    }
}

$CLICKHOUSE_CLIENT -q "CREATE DICTIONARY dict (key Int, value String) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE data)) LAYOUT(HASHED()) LIFETIME(0)"
uuid="$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.dictionaries WHERE database = '$CLICKHOUSE_DATABASE' AND name = 'dict'")"

echo 'status before reload'
get_dictionary_status "$uuid"

# source table does not exists
# NOTE: when dictionary does not exist it produce BAD_ARGUMENTS error, so using UNKNOWN_TABLE is safe
$CLICKHOUSE_CLIENT -n -q "SYSTEM RELOAD DICTIONARY dict -- { serverError UNKNOWN_TABLE }"
echo 'status after reload'
get_dictionary_status "$uuid"

# create source
$CLICKHOUSE_CLIENT -q "CREATE TABLE data (key Int, value String) Engine=Null"
$CLICKHOUSE_CLIENT -q "SYSTEM RELOAD DICTIONARY dict"
echo 'status after reload, table exists'
get_dictionary_status "$uuid"

# remove dictionary
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY dict"
$CLICKHOUSE_CLIENT -q "DROP TABLE data"
echo 'status after drop'
get_dictionary_status "$uuid"
