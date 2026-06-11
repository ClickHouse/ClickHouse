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

# A single MsgPack object: a chain of size-1 fixarrays (0x91) ending in a fixint.
# Case 1 - default max_parser_depth (1000): a moderate depth that is above the limit. The explicit
# limit rejects it early, before building the deep type. 8000 is also within the range that used to
# overflow the native stack downstream, so this also pins the moderate-depth regression.
python3 -c "open('${TMP_DIR}/deep.msgpack','wb').write(b'\x91'*8000 + b'\x00')"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep.msgpack', MsgPack) SETTINGS input_format_msgpack_number_of_columns=1" 2>&1 \
    | grep -qF 'TOO_DEEP_RECURSION' && echo 'explicit_limit OK' || echo 'explicit_limit FAIL'

# Case 2 - max_parser_depth raised far above the nesting and nullable finalization enabled, so the
# explicit limit does not short-circuit and the inferred type also flows through makeNullableRecursively.
# The depth is large enough to exhaust the native stack in any build, so the checkStackSize backstop
# (in getDataType and/or the shared adjustNullableRecursively walk) must reject it as TOO_DEEP_RECURSION
# instead of segfaulting (this is the shape that previously exited 139).
python3 -c "open('${TMP_DIR}/deep_big.msgpack','wb').write(b'\x91'*300000 + b'\x00')"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep_big.msgpack', MsgPack) SETTINGS input_format_msgpack_number_of_columns=1, max_parser_depth=10000000, schema_inference_make_columns_nullable=1" 2>&1 \
    | grep -qF 'TOO_DEEP_RECURSION' && echo 'stack_backstop OK' || echo 'stack_backstop FAIL'
