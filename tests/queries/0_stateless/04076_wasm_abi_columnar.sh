#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS str_byte_sum;
DROP FUNCTION IF EXISTS bytes_equal;
DROP FUNCTION IF EXISTS add_offset;
DROP FUNCTION IF EXISTS bytes_reverse;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_abi', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

CREATE OR REPLACE FUNCTION str_byte_sum
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi' :: 'str_byte_sum_col'
    ARGUMENTS (s String) RETURNS UInt64
    DETERMINISTIC;

CREATE OR REPLACE FUNCTION bytes_equal
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi' :: 'bytes_equal_col'
    ARGUMENTS (a String, b String) RETURNS UInt8
    DETERMINISTIC;

CREATE OR REPLACE FUNCTION add_offset
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi' :: 'add_offset_col'
    ARGUMENTS (s String, n UInt64) RETURNS UInt64
    DETERMINISTIC;

CREATE OR REPLACE FUNCTION bytes_reverse
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi' :: 'bytes_reverse_col'
    ARGUMENTS (s String) RETURNS Nullable(String)
    DETERMINISTIC;

-- Basic scalar: sum of ASCII bytes in 'abc' = 97+98+99 = 294
SELECT str_byte_sum('abc');

-- Empty string → 0
SELECT str_byte_sum('');

-- Multi-row batch
SELECT str_byte_sum(s) FROM (SELECT * FROM (VALUES ('a'), ('ab'), ('abc')) AS t(s));

-- Predicate: equal / not-equal
SELECT bytes_equal('hello', 'hello'), bytes_equal('hello', 'world'), bytes_equal('', '');

-- Constant second argument (COL_IS_CONST path): add_offset with literal n
SELECT add_offset('abc', toUInt64(10));   -- 294 + 10 = 304
SELECT add_offset('abc', toUInt64(0));    -- 294

-- Multi-row with const column
SELECT add_offset(s, toUInt64(1)) FROM (SELECT * FROM (VALUES ('a'), ('b'), ('c')) AS t(s));

-- Nullable string output: non-empty reverses, empty → NULL
SELECT bytes_reverse('abc');        -- 'cba'
SELECT bytes_reverse('');           -- NULL
SELECT isNull(bytes_reverse(''));   -- 1

-- Multi-row nullable output
SELECT bytes_reverse(s) FROM (SELECT * FROM (VALUES ('hello'), (''), ('x')) AS t(s));

-- Verify correct string handling: 'hello' byte-sum = 104+101+108+108+111 = 532
SELECT str_byte_sum('hello');

EOF
