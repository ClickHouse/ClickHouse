#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

CASE_VARIANTS_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_case_variants.arrow"
LOWER_ONLY_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_lower_only.arrow"
PROFILE_EVENTS_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.profile_events"
trap 'rm -f "${CASE_VARIANTS_FILE}" "${LOWER_ONLY_FILE}" "${PROFILE_EVENTS_FILE}"' EXIT

${CLICKHOUSE_LOCAL} --query "SELECT number::UInt64 AS \`value\`, (number + 10)::UInt64 AS \`VALUE\`, repeat('x', 1000) AS payload FROM numbers(1000) FORMAT Arrow" > "${CASE_VARIANTS_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT number::UInt64 AS value, repeat('x', 1000) AS payload FROM numbers(1000) FORMAT Arrow" > "${LOWER_ONLY_FILE}"

get_selected_bytes()
{
    ${CLICKHOUSE_LOCAL} --print-profile-events --query "$1" > /dev/null 2> "${PROFILE_EVENTS_FILE}"
    sed -n 's/.*SelectedBytes: \([0-9][0-9]*\).*/\1/p' "${PROFILE_EVENTS_FILE}" | tail -1
}

echo "Exact case variants"
${CLICKHOUSE_LOCAL} --query "SELECT \`value\`, \`VALUE\` FROM file('${CASE_VARIANTS_FILE}', 'Arrow', '\`value\` UInt64, \`VALUE\` UInt64, payload String') ORDER BY \`value\` LIMIT 3 SETTINGS input_format_arrow_case_insensitive_column_matching = 0"

case_variants_projected_bytes=$(get_selected_bytes "SELECT \`value\`, \`VALUE\` FROM file('${CASE_VARIANTS_FILE}', 'Arrow', '\`value\` UInt64, \`VALUE\` UInt64, payload String') FORMAT Null SETTINGS input_format_arrow_case_insensitive_column_matching = 0")
case_variants_all_bytes=$(get_selected_bytes "SELECT \`value\`, \`VALUE\`, payload FROM file('${CASE_VARIANTS_FILE}', 'Arrow', '\`value\` UInt64, \`VALUE\` UInt64, payload String') FORMAT Null SETTINGS input_format_arrow_case_insensitive_column_matching = 0")

if (( case_variants_projected_bytes * 4 >= case_variants_all_bytes ))
then
    echo "Expected projected Arrow read for case variants to be much smaller than full read, got projected=${case_variants_projected_bytes}, all=${case_variants_all_bytes}"
    exit 1
fi

echo "Case-insensitive access"
${CLICKHOUSE_LOCAL} --query "SELECT VALUE FROM file('${LOWER_ONLY_FILE}', 'Arrow', 'VALUE UInt64, payload String') ORDER BY VALUE LIMIT 3 SETTINGS input_format_arrow_case_insensitive_column_matching = 1"

case_insensitive_projected_bytes=$(get_selected_bytes "SELECT VALUE FROM file('${LOWER_ONLY_FILE}', 'Arrow', 'VALUE UInt64, payload String') FORMAT Null SETTINGS input_format_arrow_case_insensitive_column_matching = 1")
case_insensitive_all_bytes=$(get_selected_bytes "SELECT VALUE, payload FROM file('${LOWER_ONLY_FILE}', 'Arrow', 'VALUE UInt64, payload String') FORMAT Null SETTINGS input_format_arrow_case_insensitive_column_matching = 1")

if (( case_insensitive_projected_bytes * 4 >= case_insensitive_all_bytes ))
then
    echo "Expected projected Arrow read for case-insensitive access to be much smaller than full read, got projected=${case_insensitive_projected_bytes}, all=${case_insensitive_all_bytes}"
    exit 1
fi
