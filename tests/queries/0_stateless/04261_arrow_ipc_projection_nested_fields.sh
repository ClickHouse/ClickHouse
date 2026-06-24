#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrow"
PROFILE_EVENTS_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.profile_events"
trap 'rm -f "${DATA_FILE}" "${PROFILE_EVENTS_FILE}"' EXIT

${CLICKHOUSE_LOCAL} --query "SELECT CAST((number::UInt64, (number + 100)::UInt64), 'Tuple(x UInt64, y UInt64)') AS nested, repeat('x', 1000) AS payload FROM numbers(1000) FORMAT Arrow" > "${DATA_FILE}"

get_selected_bytes()
{
    ${CLICKHOUSE_LOCAL} --print-profile-events --query "$1" > /dev/null 2> "${PROFILE_EVENTS_FILE}"
    sed -n 's/.*SelectedBytes: \([0-9][0-9]*\).*/\1/p' "${PROFILE_EVENTS_FILE}" | tail -1
}

echo "Nested subcolumns"
${CLICKHOUSE_LOCAL} --query "SELECT nested.x, nested.y FROM file('${DATA_FILE}', 'Arrow', 'nested Tuple(x UInt64, y UInt64), payload String') ORDER BY nested.x LIMIT 3"

nested_projected_bytes=$(get_selected_bytes "SELECT nested.x FROM file('${DATA_FILE}', 'Arrow', 'nested Tuple(x UInt64, y UInt64), payload String') FORMAT Null")
all_bytes=$(get_selected_bytes "SELECT nested.x, payload FROM file('${DATA_FILE}', 'Arrow', 'nested Tuple(x UInt64, y UInt64), payload String') FORMAT Null")

if (( nested_projected_bytes * 4 >= all_bytes ))
then
    echo "Expected projected Arrow read for nested subcolumn to be much smaller than full read, got projected=${nested_projected_bytes}, all=${all_bytes}"
    exit 1
fi
