#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental while its wire layout is still evolving.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS add_offset_buffered;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_buffered_cast';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_abi_buffered_cast', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

-- add_offset_col(s String, n UInt64) reads n as a raw 8-byte UInt64 from the wire.
-- Declared as BUFFERED_V1 + serialization_format = 'ColumnBinary' to exercise the
-- getArgumentsBlock cast: without casting the UInt8 constant argument to the
-- declared UInt64 before serialization, the wire would carry a 1-byte COL_FIXED8
-- value where the guest expects 8 bytes, reading past it into unrelated frame
-- bytes instead of the correct value.
CREATE OR REPLACE FUNCTION add_offset_buffered
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'columnar_abi_buffered_cast' :: 'add_offset_col'
    ARGUMENTS (s String, n UInt64) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';

-- byte-sum('abc') = 97+98+99 = 294; 294 + 250 = 544.
SELECT add_offset_buffered('abc', toUInt8(250));

-- Same check across a multi-row batch with a per-row UInt8 argument.
SELECT add_offset_buffered(s, toUInt8(n))
FROM (SELECT * FROM (VALUES ('a', 10), ('bb', 20), ('ccc', 30)) AS t(s, n));

DROP FUNCTION IF EXISTS add_offset_buffered;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_buffered_cast';
EOF
