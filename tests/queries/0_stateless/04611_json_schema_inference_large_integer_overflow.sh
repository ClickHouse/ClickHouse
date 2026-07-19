#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

check_overflow_error()
{
    local label=$1
    local query=$2
    local output

    output=$($CLICKHOUSE_LOCAL --query "$query" 2>&1) || true
    if grep -qF "CANNOT_PARSE_NUMBER" <<< "$output" \
        && grep -qF "Cannot infer type of JSON integer" <<< "$output"; then
        echo "$label: overflow error is clear"
    else
        echo "$label: expected clear CANNOT_PARSE_NUMBER overflow error, got:"
        printf '%s\n' "$output" | head -5
    fi
}

$CLICKHOUSE_LOCAL --query "DESC format(JSONEachRow, '{\"num\":1}') SETTINGS input_format_json_read_numbers_as_strings=0"

check_overflow_error "positive with read_numbers_as_strings=0" \
    "DESC format(JSONEachRow, '{\"num\":2942420318599003496251392}') SETTINGS input_format_json_read_numbers_as_strings=0"

# Default setting is read_numbers_as_strings=1; oversized bare integers still must not
# fail with a cryptic mid-token parse error.
check_overflow_error "positive with default settings" \
    "DESC format(JSONEachRow, '{\"num\":2942420318599003496251392}')"

check_overflow_error "negative overflow" \
    "DESC format(JSONEachRow, '{\"num\":-9223372036854775809}') SETTINGS input_format_json_read_numbers_as_strings=0"

check_overflow_error "uint64 max + 1" \
    "DESC format(JSONEachRow, '{\"num\":18446744073709551616}') SETTINGS input_format_json_read_numbers_as_strings=0"
