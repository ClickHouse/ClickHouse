#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS array_of_len;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_nullable_arg';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_abi_nullable_arg', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

-- array_of_len_col(s String) -> Array(UInt64) returns [byte_len(s)] per row. Since
-- Array(UInt64) cannot be inside Nullable, useDefaultImplementationForNulls() is false
-- and the analyzer passes a genuinely Nullable(String) argument through unmodified
-- against the declared non-nullable String parameter. executeColumnar must cast this
-- to Nullable(String) (not plain String) before serialization, or the cast itself
-- throws CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN on the first NULL row.
CREATE OR REPLACE FUNCTION array_of_len
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_nullable_arg' :: 'array_of_len_col'
    ARGUMENTS (s String) RETURNS Array(UInt64)
    DETERMINISTIC;

SELECT s, array_of_len(s)
FROM (SELECT 'abc' AS s FROM numbers(1) UNION ALL SELECT NULL FROM numbers(1))
ORDER BY s;

DROP FUNCTION IF EXISTS array_of_len;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_nullable_arg';
EOF

# validateColumnarV1SupportedType must deny-by-default: any type family it doesn't
# explicitly know how to encode (Dynamic, JSON/Object, ...) must be rejected up front
# rather than silently falling through as "supported" and failing deep inside
# buildColDescriptor once the first block is actually serialized.
${CLICKHOUSE_CLIENT} --query "SELECT 1::Dynamic AS d FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "type is not supported"

${CLICKHOUSE_CLIENT} --query "SELECT '{\"a\":1}'::JSON AS j FROM numbers(1) FORMAT ColumnBinary" 2>&1 \
    | grep -o "type is not supported"
