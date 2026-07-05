#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# COLUMNAR_V1 argument columns must be cast to the declared argument type before
# serialization (mirroring BUFFERED_V1's getArgumentsBlock cast), and executeImpl must
# apply webassembly_udf_max_input_block_size splitting the same way the buffered path
# does, instead of passing every row into a single guest buffer.

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS add_offset_cast_split;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_cast_split';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_abi_cast_split', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

-- add_offset_col(s String, n UInt64) reads n as a raw 8-byte UInt64 from the wire.
-- Without casting the actual UInt8 argument to the declared UInt64 before serialization,
-- the wire would carry a 1-byte COL_FIXED8 value where the guest expects 8 bytes.
CREATE OR REPLACE FUNCTION add_offset_cast_split
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_cast_split' :: 'add_offset_col'
    ARGUMENTS (s String, n UInt64) RETURNS UInt64
    DETERMINISTIC;

-- byte-sum('abc') = 97+98+99 = 294; 294 + 250 = 544.
SELECT add_offset_cast_split('abc', toUInt8(250));

-- Force a 5-row call to split into 3 batches of at most 2 rows each; the result must
-- still be exactly the same as an unsplit call would produce, in the original row order.
SELECT add_offset_cast_split(s, n)
FROM (SELECT 'a' AS s, 10 AS n FROM numbers(1)
      UNION ALL SELECT 'bb', 20 FROM numbers(1)
      UNION ALL SELECT 'ccc', 30 FROM numbers(1)
      UNION ALL SELECT 'dddd', 40 FROM numbers(1)
      UNION ALL SELECT 'eeeee', 50 FROM numbers(1))
ORDER BY n
SETTINGS webassembly_udf_max_input_block_size = 2;

DROP FUNCTION IF EXISTS add_offset_cast_split;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_cast_split';
EOF
