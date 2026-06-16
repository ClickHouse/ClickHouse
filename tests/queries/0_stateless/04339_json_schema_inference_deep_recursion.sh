#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: JSON schema inference recurses over nested arrays/objects in
# tryInferDataTypeForSingleFieldImpl / tryInferArray / tryReadJSONObject. It was guarded only by
# `depth > max_parser_depth`, with no checkStackSize backstop - so raising max_parser_depth let a
# deeply nested value overflow the native stack (SIGSEGV). It must be rejected as TOO_DEEP_RECURSION.

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

# One JSON row whose field x is an array nested 200000 deep. max_parser_depth is raised far above
# the nesting so the explicit limit does not short-circuit; the depth is large enough to exhaust the
# native stack in any build, so the checkStackSize backstop must reject it (build-independent).
python3 -c "
N = 200000
open('${TMP_DIR}/deep.jsonl', 'wb').write(b'{\"x\":' + b'[' * N + b'1' + b']' * N + b'}\n')
"

$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep.jsonl', JSONEachRow) SETTINGS max_parser_depth=100000000 FORMAT Null" 2>&1 \
    | expect_contains json_inference_backstop TOO_DEEP_RECURSION

# max_parser_depth=0 means unlimited (matching the SQL parser), so a shallow value must still be
# inferred normally rather than rejected by the explicit limit.
echo '{"x":1}' > "${TMP_DIR}/shallow.jsonl"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/shallow.jsonl', JSONEachRow) SETTINGS max_parser_depth=0 FORMAT Null" 2>&1 \
    | expect_absent unlimited_depth TOO_DEEP_RECURSION
