#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=tab_full_attach

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE} SYNC" >/dev/null 2>&1
}

trap cleanup EXIT
cleanup

# A full-definition ATTACH is a newly submitted definition and must be validated like CREATE.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
output=$($CLICKHOUSE_CLIENT --send_logs_level fatal -q "
    ATTACH TABLE ${TABLE} UUID '${uuid}'
    (
        t Array(Array(String)),
        INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree ORDER BY tuple();" 2>&1)

if grep -q -F 'BAD_ARGUMENTS' <<< "$output"; then
    echo 'full-definition ATTACH rejected'
else
    echo 'full-definition ATTACH was not rejected with BAD_ARGUMENTS'
fi
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE} SYNC"

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
output=$($CLICKHOUSE_CLIENT --send_logs_level fatal -q "
    ATTACH TABLE ${TABLE} UUID '${uuid}'
    (
        t Array(Array(FixedString(8))),
        INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree ORDER BY tuple();" 2>&1)

if grep -q -F 'BAD_ARGUMENTS' <<< "$output"; then
    echo 'full-definition FixedString ATTACH rejected'
else
    echo 'full-definition FixedString ATTACH was not rejected with BAD_ARGUMENTS'
fi
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE} SYNC"
