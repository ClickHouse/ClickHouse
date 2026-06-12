#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: ORC schema inference (parseORCType) recursed over nested LIST/MAP/STRUCT types
# with no depth limit and ignoring max_parser_depth. A small but deeply nested ORC schema made
# schema inference grind for tens of seconds (and could overflow the native stack), uncancellable.
# Nesting must now be rejected as TOO_DEEP_RECURSION.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# An empty ORC file whose single column type is list nested 8000 levels deep (Array(Array(...))).
# 8000 is far above the default max_parser_depth (1000), so the rejection is build-independent.
python3 -c "
import pyarrow as pa, pyarrow.orc as orc
t = pa.int32()
for _ in range(8000):
    t = pa.list_(t)
orc.write_table(pa.table({'x': pa.array([], type=t)}), '${TMP_DIR}/deep.orc')
"

$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep.orc', ORC)" 2>&1 \
    | grep -qF 'TOO_DEEP_RECURSION' && echo 'orc_depth_limit OK' || echo 'orc_depth_limit FAIL'
