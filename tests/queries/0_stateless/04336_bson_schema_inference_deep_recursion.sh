#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: BSONEachRow schema inference recursed over nested documents/arrays in
# getDataTypesFromBSONDocument <-> getDataTypeFromBSONField without a depth limit, so a tiny,
# deeply nested BSON document overflowed the native stack and crashed the process. Nesting must
# now be rejected as TOO_DEEP_RECURSION, never crash.

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

# Build a single BSON document (one row) whose field "x" is an array nested N levels deep, ending
# in an int32. Each level is a BSON document {"0": <array>}, adding 8 bytes, so the whole input is
# a few MB even for N=300000. Built in O(N) by precomputing each level's length prefix and joining
# once (a naive bottom-up prepend would be O(N^2) and far too slow).
gen_deep_bson() {
    local depth="$1" out="$2"
    python3 - "$depth" "$out" <<'PYEOF'
import struct, sys
depth = int(sys.argv[1])
# Innermost document: one int32 field "0" = 0.
inner_elem = b'\x10' + b'0\x00' + struct.pack('<i', 0)
inner_doc = struct.pack('<i', 4 + len(inner_elem) + 1) + inner_elem + b'\x00'
# Each wrapping array level adds 8 bytes (size(4) + type(1) + "0\0"(2) + child + end(1) = child+8).
# Emit the prefixes outermost-first, then the inner doc, then one document-end byte per level.
parts = []
for k in range(depth):  # k=0 is the outermost array level
    size_k = len(inner_doc) + 8 * (depth - k)
    parts.append(struct.pack('<i', size_k) + b'\x04' + b'0\x00')
array_blob = b''.join(parts) + inner_doc + b'\x00' * depth
# Wrap in the top-level row document under field "x".
row_elem = b'\x04' + b'x\x00' + array_blob
row = struct.pack('<i', 4 + len(row_elem) + 1) + row_elem + b'\x00'
open(sys.argv[2], 'wb').write(row)
PYEOF
}

# Case 1 - default max_parser_depth (1000): a moderate depth above the limit, rejected early before
# building the type. 8000 is also within the range that used to overflow the native stack.
gen_deep_bson 8000 "${TMP_DIR}/deep.bson"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep.bson', BSONEachRow)" 2>&1 \
    | expect_contains explicit_limit TOO_DEEP_RECURSION

# Case 2 - max_parser_depth raised far above the nesting and nullable finalization enabled, so the
# explicit limit does not short-circuit. The depth is large enough to exhaust the native stack in
# any build, so the checkStackSize backstop must reject it as TOO_DEEP_RECURSION, never segfault.
gen_deep_bson 300000 "${TMP_DIR}/deep_big.bson"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep_big.bson', BSONEachRow) SETTINGS max_parser_depth=10000000, schema_inference_make_columns_nullable=1" 2>&1 \
    | expect_contains stack_backstop TOO_DEEP_RECURSION

# Case 3 - max_parser_depth=0 means unlimited (matching the SQL parser), so a shallow document must
# still be inferred normally rather than rejected by the explicit limit.
gen_deep_bson 2 "${TMP_DIR}/shallow.bson"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/shallow.bson', BSONEachRow) SETTINGS max_parser_depth=0" 2>&1 \
    | expect_absent unlimited_depth TOO_DEEP_RECURSION
