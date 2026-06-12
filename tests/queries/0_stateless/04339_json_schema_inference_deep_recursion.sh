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

# One JSON row whose field x is an array nested 200000 deep. max_parser_depth is raised far above
# the nesting so the explicit limit does not short-circuit; the depth is large enough to exhaust the
# native stack in any build, so the checkStackSize backstop must reject it (build-independent).
python3 -c "
N = 200000
open('${TMP_DIR}/deep.jsonl', 'wb').write(b'{\"x\":' + b'[' * N + b'1' + b']' * N + b'}\n')
"

$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep.jsonl', JSONEachRow) SETTINGS max_parser_depth=100000000 FORMAT Null" 2>&1 \
    | grep -qF 'TOO_DEEP_RECURSION' && echo 'json_inference_backstop OK' || echo 'json_inference_backstop FAIL'
