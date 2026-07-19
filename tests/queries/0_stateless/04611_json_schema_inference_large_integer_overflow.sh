#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

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

$CLICKHOUSE_LOCAL --query "DESC format(JSONEachRow, '{\"num\":1}') SETTINGS input_format_json_read_numbers_as_strings=0" | cut -f 1,2

check_overflow_error "positive with read_numbers_as_strings=0" \
    "DESC format(JSONEachRow, '{\"num\":2942420318599003496251392}') SETTINGS input_format_json_read_numbers_as_strings=0"

# Default setting is read_numbers_as_strings=1: oversized bare integers must infer as String
# and preserve exact digits (not fail mid-token, and not round via Float64).
$CLICKHOUSE_LOCAL --query "DESC format(JSONEachRow, '{\"num\":2942420318599003496251392,\"name\":\"Matt\"}')" | cut -f 1,2
$CLICKHOUSE_LOCAL --query "SELECT toTypeName(num), num FROM format(JSONEachRow, '{\"num\":2942420318599003496251392}')"
$CLICKHOUSE_LOCAL --query "SELECT toTypeName(num), num FROM format(JSONEachRow, '{\"num\":-9223372036854775809}')"
$CLICKHOUSE_LOCAL --query "SELECT toTypeName(num), num FROM format(JSONEachRow, '{\"num\":18446744073709551616}')"
$CLICKHOUSE_LOCAL --query "SELECT dynamicType(num), num FROM format(JSONEachRow, 'num Dynamic', '{\"num\":2942420318599003496251392}')"

# File-based schema inference uses a stream ReadBuffer (Peekable path), not ReadBufferFromString.
# Oversized int must leave the buffer positioned so a later field in the same object is still parsed.
cat > "${TMP_DIR}/overflow_with_sibling.jsonl" <<'EOF'
{"num":2942420318599003496251392,"name":"Matt"}
EOF
$CLICKHOUSE_LOCAL --query "DESC file('${TMP_DIR}/overflow_with_sibling.jsonl', JSONEachRow)" | cut -f 1,2
$CLICKHOUSE_LOCAL --query "SELECT toTypeName(num), num, name FROM file('${TMP_DIR}/overflow_with_sibling.jsonl', JSONEachRow)"

# Mixed in-range Int64 and oversized integer across rows must unify to String.
cat > "${TMP_DIR}/mixed_int_and_overflow.jsonl" <<'EOF'
{"x":1}
{"x":2942420318599003496251392}
EOF
$CLICKHOUSE_LOCAL --query "DESC file('${TMP_DIR}/mixed_int_and_overflow.jsonl', JSONEachRow)" | cut -f 1,2
$CLICKHOUSE_LOCAL --query "SELECT toTypeName(x), x FROM file('${TMP_DIR}/mixed_int_and_overflow.jsonl', JSONEachRow) ORDER BY x"

check_overflow_error "negative overflow" \
    "DESC format(JSONEachRow, '{\"num\":-9223372036854775809}') SETTINGS input_format_json_read_numbers_as_strings=0"

check_overflow_error "uint64 max + 1" \
    "DESC format(JSONEachRow, '{\"num\":18446744073709551616}') SETTINGS input_format_json_read_numbers_as_strings=0"
