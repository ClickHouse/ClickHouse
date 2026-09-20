#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: MsgPack schema inference recursed over nested arrays/maps without a depth limit.
# msgpack::unpack builds the whole object tree iteratively (on the heap), so a tiny, deeply nested
# object overflowed the native stack and crashed the process - both in MsgPackSchemaReader::getDataType
# and, for moderate depths that slipped past it, in the shared makeNullableRecursively walk over the
# inferred type. Nesting must now be rejected as TOO_DEEP_RECURSION, never crash.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# Reads a query's combined output from stdin. If it contains $2, print a stable "<label>: <marker>"
# line; otherwise print the actual output so a CI failure shows what really happened instead of FAIL.
expect_contains() {
    local label="$1" marker="$2" out
    out=$(cat)
    if printf '%s\n' "$out" | grep -qF "$marker"; then
        echo "$label: $marker"
    else
        echo "$label: expected '$marker', got:"
        printf '%s\n' "$out" | head -3
    fi
}

# Same, but asserts the marker is ABSENT (used for the "accepted" cases).
expect_absent() {
    local label="$1" marker="$2" out
    out=$(cat)
    if printf '%s\n' "$out" | grep -qF "$marker"; then
        echo "$label: unexpectedly found '$marker':"
        printf '%s\n' "$out" | head -3
    else
        echo "$label: accepted"
    fi
}

# A single MsgPack object: a chain of size-1 fixarrays (0x91) ending in a fixint.
# Case 1 - default max_parser_depth (1000): a moderate depth that is above the limit. The explicit
# limit rejects it early, before building the deep type. 8000 is also within the range that used to
# overflow the native stack downstream, so this also pins the moderate-depth regression.
python3 -c "open('${TMP_DIR}/deep.msgpack','wb').write(b'\x91'*8000 + b'\x00')"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep.msgpack', MsgPack) SETTINGS input_format_msgpack_number_of_columns=1" 2>&1 \
    | expect_contains explicit_limit TOO_DEEP_RECURSION

# Case 2 - max_parser_depth raised far above the nesting and nullable finalization enabled, so the
# explicit limit does not short-circuit and the inferred type also flows through makeNullableRecursively.
# The depth is large enough to exhaust the native stack in any build, so the checkStackSize backstop
# (in getDataType and/or the shared adjustNullableRecursively walk) must reject it as TOO_DEEP_RECURSION
# instead of segfaulting (this is the shape that previously exited 139).
python3 -c "open('${TMP_DIR}/deep_big.msgpack','wb').write(b'\x91'*300000 + b'\x00')"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep_big.msgpack', MsgPack) SETTINGS input_format_msgpack_number_of_columns=1, max_parser_depth=10000000, schema_inference_make_columns_nullable=1" 2>&1 \
    | expect_contains stack_backstop TOO_DEEP_RECURSION

# Case 3 - multi-row inference merges per-row types via chooseResultColumnType -> IDataType::equals,
# another recursion over the inferred type. Two deep objects back-to-back, with the limit raised and
# nullable finalization on, must still be rejected as TOO_DEEP_RECURSION, never crash (exit 139).
python3 -c "obj = b'\x91'*300000 + b'\x00'; open('${TMP_DIR}/deep_two.msgpack','wb').write(obj + obj)"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep_two.msgpack', MsgPack) SETTINGS input_format_msgpack_number_of_columns=1, max_parser_depth=10000000, schema_inference_make_columns_nullable=1, input_format_max_rows_to_read_for_schema_inference=10" 2>&1 \
    | expect_contains multirow_merge TOO_DEEP_RECURSION

# Case 4 - max_parser_depth=0 means unlimited (matching the SQL parser), so it must NOT turn the
# explicit limit into "reject everything". A shallow object must still be inferred normally.
python3 -c "open('${TMP_DIR}/shallow.msgpack','wb').write(b'\x91\x91\x00')"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/shallow.msgpack', MsgPack) SETTINGS input_format_msgpack_number_of_columns=1, max_parser_depth=0" 2>&1 \
    | expect_absent unlimited_depth TOO_DEEP_RECURSION
