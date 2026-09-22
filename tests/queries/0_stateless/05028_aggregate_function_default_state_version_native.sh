#!/usr/bin/env bash
set -e -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=aggregate_function_default_state_version_native
STATE_FILE=$(mktemp "${CLICKHOUSE_TMP}/${TABLE}.XXXXXX")
trap 'rm -f -- "$STATE_FILE"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS $TABLE;
    CREATE TABLE $TABLE
    (
        state AggregateFunction(sumMap, Array(UInt8), Array(Decimal(9, 2)))
    )
    ENGINE = Memory;
"

$CLICKHOUSE_CURL --fail "$CLICKHOUSE_URL" \
    --data-binary 'SELECT sumMapState([toUInt8(1)], [toDecimal32(1, 2)]) AS state FROM numbers(1) FORMAT Native' \
    > "$STATE_FILE"

if LC_ALL=C grep -aqF 'AggregateFunction(1, sumMap' "$STATE_FILE"; then
    echo "version 1"
    exit 1
fi

if ! LC_ALL=C grep -aqF 'AggregateFunction(sumMap' "$STATE_FILE"; then
    echo "missing sumMap state type"
    exit 1
fi

echo "version 0"

$CLICKHOUSE_CLIENT -q "INSERT INTO $TABLE FORMAT Native" < "$STATE_FILE" >/dev/null
