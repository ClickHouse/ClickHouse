#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for deeply nested ORC schemas, covering two layers of defense:
#
# 1. ClickHouse schema inference (parseORCType) recursed over nested LIST/MAP/STRUCT types with no
#    depth limit and ignoring max_parser_depth. A small but deeply nested ORC schema made inference
#    grind (and could overflow the native stack), uncancellable. It must now be rejected as
#    TOO_DEEP_RECURSION once the nesting exceeds max_parser_depth.
#
# 2. The Apache ORC C++ library (convertType) builds its own orc::Type tree by recursing over the
#    file footer before any ClickHouse code runs. A pathologically deep file used to overflow the
#    native stack there (crashing the process / triggering the OOM killer). The library now bounds
#    the nesting depth and rejects such a file instead of crashing.

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

gen_orc() { # $1=depth $2=path
    python3 -c "
import pyarrow as pa, pyarrow.orc as orc
t = pa.int32()
for _ in range($1):
    t = pa.list_(t)
orc.write_table(pa.table({'x': pa.array([], type=t)}), '$2')
"
}

# (1) A moderate file (200 levels) that the ORC library builds fine, with max_parser_depth lowered
# below the nesting so parseORCType rejects it deterministically and build-independently.
gen_orc 200 "${TMP_DIR}/moderate.orc"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/moderate.orc', ORC) SETTINGS max_parser_depth = 100" 2>&1 \
    | expect_contains parser_depth_limit TOO_DEEP_RECURSION

# (2) A file nested past the ORC library's own limit. Even with max_parser_depth raised so high that
# parseORCType would accept it, the library must reject it (without crashing). The library throws
# before recursing past its limit, so this is safe to run.
gen_orc 3000 "${TMP_DIR}/deep.orc"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep.orc', ORC) SETTINGS max_parser_depth = 100000" 2>&1 \
    | expect_contains orc_library_depth_limit "nesting depth exceeds the limit"

# (3) max_parser_depth=0 means unlimited (matching the SQL parser), so a moderately nested file must
# still be inferred normally rather than rejected by the explicit limit.
gen_orc 50 "${TMP_DIR}/shallow.orc"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/shallow.orc', ORC) SETTINGS max_parser_depth = 0" 2>&1 \
    | expect_absent unlimited_depth TOO_DEEP_RECURSION
