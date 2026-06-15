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

# (1) A moderate file (200 levels) that the ORC library builds fine, with max_parser_depth lowered
# below the nesting so parseORCType rejects it deterministically and build-independently.
python3 -c "
import pyarrow as pa, pyarrow.orc as orc
t = pa.int32()
for _ in range(200):
    t = pa.list_(t)
orc.write_table(pa.table({'x': pa.array([], type=t)}), '${TMP_DIR}/moderate.orc')
"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/moderate.orc', ORC) SETTINGS max_parser_depth = 100" 2>&1 \
    | grep -qF 'TOO_DEEP_RECURSION' && echo 'parser_depth_limit OK' || echo 'parser_depth_limit FAIL'

# (2) A file nested past the ORC library's own limit. Even with max_parser_depth raised so high that
# parseORCType would accept it, the library must reject it (without crashing). The library throws
# before recursing past its limit, so this is safe to run.
python3 -c "
import pyarrow as pa, pyarrow.orc as orc
t = pa.int32()
for _ in range(3000):
    t = pa.list_(t)
orc.write_table(pa.table({'x': pa.array([], type=t)}), '${TMP_DIR}/deep.orc')
"
$CLICKHOUSE_LOCAL --query "DESC TABLE file('${TMP_DIR}/deep.orc', ORC) SETTINGS max_parser_depth = 100000" 2>&1 \
    | grep -qF 'nesting depth exceeds the limit' && echo 'orc_library_depth_limit OK' || echo 'orc_library_depth_limit FAIL'
