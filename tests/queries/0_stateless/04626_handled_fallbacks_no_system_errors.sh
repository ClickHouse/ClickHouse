#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: `system.errors` keeps only the latest query ID for each error code.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

query_id_prefix="04626_handled_fallback_${CLICKHOUSE_DATABASE}_${RANDOM}"

run_clean_query()
{
    local suffix=$1
    local query=$2
    local query_id="${query_id_prefix}_${suffix}"

    $CLICKHOUSE_CLIENT --query_id "$query_id" --query "$query"
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS error_log"
    $CLICKHOUSE_CLIENT --query "SELECT count() = 0 FROM system.errors WHERE query_id = '${query_id}'"
    $CLICKHOUSE_CLIENT --query "SELECT count() = 0 FROM system.error_log WHERE last_error_query_id = '${query_id}'"
}

run_clean_query format_query "SELECT formatQueryOrNull('SELECT (') IS NULL"
run_clean_query readable_size "SELECT parseReadableSizeOrNull('invalid') IS NULL"
run_clean_query case_fallback "SELECT CASE 0 WHEN 0 THEN 1::Int128 WHEN 1 THEN 2::Int128 ELSE 3::Int128 END"
run_clean_query accurate_cast "SELECT accurateCastOrNull('not_bool', 'Bool') IS NULL"
run_clean_query csv_default "SELECT x FROM format(CSV, 'x UInt64', 'bad') SETTINGS input_format_csv_use_default_on_bad_values = 1"
run_clean_query csv_skip "SELECT groupArray(x) FROM format(CSV, 'x UInt64', '1\nbad\n2') SETTINGS input_format_allow_errors_num = 1"

strict_query_id="${query_id_prefix}_strict"
if $CLICKHOUSE_CLIENT --query_id "$strict_query_id" --query "SELECT parseReadableSize('invalid')" >/dev/null 2>&1; then
    echo "The strict parser unexpectedly succeeded" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SELECT count() = 1 FROM system.errors WHERE name = 'CANNOT_PARSE_NUMBER' AND query_id = '${strict_query_id}'"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS error_log"
$CLICKHOUSE_CLIENT --query "SELECT count() = 1 FROM system.error_log WHERE error = 'CANNOT_PARSE_NUMBER' AND last_error_query_id = '${strict_query_id}'"
