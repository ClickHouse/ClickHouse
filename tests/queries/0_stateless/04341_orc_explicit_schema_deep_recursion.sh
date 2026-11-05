#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the ORC explicit-schema read path. prepareFileReader maps each requested
# column onto the file schema with traverseDownORCTypeByName / updateIncludeTypeIds, which recurse
# the file-controlled nested LIST/MAP/STRUCT tree before any stripe (and before the
# readColumnFromORCColumn guard) is reached. A deeply nested explicit type must be rejected
# without crashing; a moderately nested one must still read fine.

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

# A deeply nested ORC file: a single column of list nested 2000 levels deep (Array(Array(...))).
python3 -c "
import pyarrow as pa, pyarrow.orc as orc
t = pa.int32()
for _ in range(2000):
    t = pa.list_(t)
orc.write_table(pa.table({'x': pa.array([], type=t)}), '${TMP_DIR}/deep.orc')
"

# Reading it with an explicit type nested past the default max_parser_depth (1000) must be rejected
# (the type cannot even be parsed that deep), not crash.
deep_type=$(python3 -c "print('Array(' * 2000 + 'Int32' + ')' * 2000)")
$CLICKHOUSE_LOCAL --query "SELECT * FROM file('${TMP_DIR}/deep.orc', ORC, 'x ${deep_type}') FORMAT Null" 2>&1 \
    | expect_contains orc_explicit_deep_rejected TOO_DEEP_RECURSION

# A moderately nested explicit type over the same kind of file must still read fine.
python3 -c "
import pyarrow as pa, pyarrow.orc as orc
t = pa.int32()
for _ in range(50):
    t = pa.list_(t)
orc.write_table(pa.table({'x': pa.array([], type=t)}), '${TMP_DIR}/moderate.orc')
"
moderate_type=$(python3 -c "print('Array(' * 50 + 'Int32' + ')' * 50)")
echo "moderate_read: $($CLICKHOUSE_LOCAL --query "SELECT count() FROM file('${TMP_DIR}/moderate.orc', ORC, 'x ${moderate_type}')" 2>&1)"
