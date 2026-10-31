#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS const_answer;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_const_result';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_abi_const_result', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

-- const_answer_col always returns COL_IS_CONST regardless of num_rows. The host
-- must accept a legitimately const result column instead of rejecting it with
-- a "structure does not match declared type" error (a guest returning
-- COL_IS_CONST was compared against a non-const expected column).
CREATE OR REPLACE FUNCTION const_answer
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_const_result' :: 'const_answer_col'
    ARGUMENTS (s String) RETURNS UInt64
    DETERMINISTIC;

-- Single row.
SELECT const_answer('abc');

-- Multi-row: every row gets the broadcast const value.
SELECT const_answer(s) FROM (SELECT * FROM (VALUES ('a'), ('bb'), ('ccc')) AS t(s));

-- Aggregate over a larger batch to confirm the const value is broadcast correctly.
SELECT sum(const_answer(toString(number))) FROM numbers(100);

DROP FUNCTION IF EXISTS const_answer;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_const_result';
EOF
