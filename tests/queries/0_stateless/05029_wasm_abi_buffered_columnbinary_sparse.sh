#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ColumnBinary` is experimental until its frame header is versioned.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_column_binary_format 1"

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS sparse_byte_sum;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_sparse';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_abi_sparse', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} << 'EOF'

CREATE OR REPLACE FUNCTION sparse_byte_sum
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'columnar_abi_sparse' :: 'add_offset_col'
    ARGUMENTS (s String, n UInt64) RETURNS UInt64
    SETTINGS serialization_format = 'ColumnBinary';

DROP TABLE IF EXISTS t_columnbinary_sparse;

-- `ratio_of_defaults_for_sparse_serialization = 0` forces sparse serialization for
-- every column, so the columns reaching the format are `ColumnSparse` wrappers.
CREATE TABLE t_columnbinary_sparse (id UInt64, s String, n UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0;

INSERT INTO t_columnbinary_sparse SELECT number, '', 0 FROM numbers(20);
INSERT INTO t_columnbinary_sparse VALUES (100, 'abc', 250);

SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_columnbinary_sparse' AND column = 's'
ORDER BY serialization_kind;

-- A sparse `String` argument must reach the guest correctly. The precompute
-- (preallocation) pass in `ColumnBinaryOutputFormat` runs before `consume`
-- normalizes the wrappers, so it must strip `ColumnSparse` itself; otherwise
-- `buildColDescriptor` misses the `ColumnString` branch and throws.
SELECT sum(sparse_byte_sum(s, n)) FROM t_columnbinary_sparse;

-- Fixed-width sparse columns are sized by `sizeOfValueIfFixed`, which reports a
-- wider value for `ColumnSparse`; check a fixed-width-only signature too.
SELECT sum(sparse_byte_sum('', n)) FROM t_columnbinary_sparse;

DROP TABLE t_columnbinary_sparse;
DROP FUNCTION IF EXISTS sparse_byte_sum;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_sparse';
EOF
