#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The wide half of the COLUMNAR_V1 wire: COL_FIXEDN (7) for every fixed-width type that is not
# one of the 1/2/4/8-byte classes (FixedString(16), UUID, IPv6, Decimal128/256) and COL_LOWCARD
# (8) for top-level LowCardinality(String). The guest echoes each column back as hex, which pins
# the exact bytes the host put on the wire: the element width recovered as data_size/rows, the
# dictionary + index split, and the const / Nullable / split-batch shapes around them.
# LowCardinality(Nullable(T)) arguments and LowCardinality result types must be rejected at
# CREATE FUNCTION: the LC frame cannot represent a nullable dictionary (the NULL rides as
# dictionary row 0, byte-identical to a plain dictionary that merely starts with the default
# value), and the guest cannot emit a LowCardinality column at all.

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS wide_fs8;
DROP FUNCTION IF EXISTS wide_fs16;
DROP FUNCTION IF EXISTS wide_fs10;
DROP FUNCTION IF EXISTS wide_uuid;
DROP FUNCTION IF EXISTS wide_ip6;
DROP FUNCTION IF EXISTS wide_dec128;
DROP FUNCTION IF EXISTS wide_dec256;
DROP FUNCTION IF EXISTS wide_lc;
DROP FUNCTION IF EXISTS wide_lc_rows;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_wide_abi';
EOF

cat "${CUR_DIR}/wasm/columnar_wide_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_wide_abi', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

CREATE OR REPLACE FUNCTION wide_fs8
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_fixed_col'
    ARGUMENTS (v FixedString(8)) RETURNS String DETERMINISTIC;
CREATE OR REPLACE FUNCTION wide_fs16
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_fixed_col'
    ARGUMENTS (v FixedString(16)) RETURNS String DETERMINISTIC;
CREATE OR REPLACE FUNCTION wide_fs10
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_fixed_col'
    ARGUMENTS (v FixedString(10)) RETURNS String DETERMINISTIC;
CREATE OR REPLACE FUNCTION wide_uuid
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_fixed_col'
    ARGUMENTS (v UUID) RETURNS String DETERMINISTIC;
CREATE OR REPLACE FUNCTION wide_ip6
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_fixed_col'
    ARGUMENTS (v IPv6) RETURNS String DETERMINISTIC;
CREATE OR REPLACE FUNCTION wide_dec128
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_fixed_col'
    ARGUMENTS (v Decimal(20, 2)) RETURNS String DETERMINISTIC;
CREATE OR REPLACE FUNCTION wide_dec256
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_fixed_col'
    ARGUMENTS (v Decimal(50, 2)) RETURNS String DETERMINISTIC;
CREATE OR REPLACE FUNCTION wide_lc
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_lc_col'
    ARGUMENTS (s LowCardinality(String)) RETURNS String DETERMINISTIC;
CREATE OR REPLACE FUNCTION wide_lc_rows
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'lc_dict_rows_col'
    ARGUMENTS (s LowCardinality(String)) RETURNS UInt64 DETERMINISTIC;

DROP TABLE IF EXISTS wasm_wide;
CREATE TABLE wasm_wide
(
    id UInt64,
    fs8 FixedString(8),
    fs16 FixedString(16),
    fs10 FixedString(10),
    u UUID,
    ip IPv6,
    d128 Decimal(20, 2),
    d256 Decimal(50, 2),
    lc LowCardinality(String),
    nfs Nullable(FixedString(16))
) ENGINE = Memory;

INSERT INTO wasm_wide VALUES
    (1, 'abcdefgh', '0123456789abcdef', 'abcdefghij', '6ba7b810-9dad-11d1-80b4-00c04fc2dce2', '2001:db8::1', 123.45, 1.5, 'alpha', 'pin16valuesq'),
    (2, 'ABCDEFGH', 'FEDCBA9876543210', 'jihgfedcba', '00000000-0000-0000-0000-000000000000', '::1', -0.01, -99999999999999999999.99, 'beta', NULL),
    (3, '', toFixedString('', 16), 'abc', toUUID('00000000-0000-0000-0000-000000000000'), '::', 0.00, 0.00, 'alpha', NULL),
    (4, 'x', toFixedString('x', 16), 'x', toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff'), 'ffff::ffff', 100000000000000000.00, 0.01, 'gamma', 'four');

-- Unsplit: one batch, the reference every split below must reproduce.
SELECT id, wide_fs8(fs8), wide_fs16(fs16), wide_fs10(fs10), wide_uuid(u), wide_ip6(ip), wide_dec128(d128), wide_dec256(d256) FROM wasm_wide ORDER BY id;

-- Split batches of 2 and of 1: a COL_FIXEDN slice must recover its element width from the
-- batch's own data_size, not from the full column.
SELECT id, wide_fs16(fs16), wide_fs10(fs10), wide_dec128(d128), wide_dec256(d256) FROM wasm_wide ORDER BY id
SETTINGS webassembly_udf_max_input_block_size = 2;
SELECT id, wide_fs8(fs8), wide_uuid(u), wide_ip6(ip) FROM wasm_wide ORDER BY id
SETTINGS webassembly_udf_max_input_block_size = 1;

-- A const argument carries one stored row whatever the row count.
SELECT wide_fs16(toFixedString('CONST!!CONST!!', 16)), wide_fs10(toFixedString('CONST10', 10)), wide_uuid(toUUID('00112233-4455-6677-8899-aabbccddeeff'));

-- Nullable(FixedString(16)) against a plain FixedString(16) parameter: the null row is
-- propagated by the framework, the non-null rows arrive as COL_FIXEDN.
SELECT id, wide_fs16(nfs) FROM wasm_wide ORDER BY id;

-- LowCardinality(String): dictionary + index decode, dictionary row counts, and the same
-- column split (each batch re-serializes its own dictionary).
SELECT id, wide_lc(lc) FROM wasm_wide ORDER BY id;
SELECT id, wide_lc_rows(lc) FROM wasm_wide ORDER BY id;
SELECT id, wide_lc(lc) FROM wasm_wide ORDER BY id
SETTINGS webassembly_udf_max_input_block_size = 2;

-- A materialized String column against a declared LowCardinality(String) parameter: the
-- runtime cast builds the dictionary up, and the frame goes out as COL_LOWCARD.
SELECT number, wide_lc(toString(number % 3 + 1)) FROM numbers(5) AS number ORDER BY number;

DROP TABLE wasm_wide;
DROP FUNCTION wide_fs8;
DROP FUNCTION wide_fs16;
DROP FUNCTION wide_fs10;
DROP FUNCTION wide_uuid;
DROP FUNCTION wide_ip6;
DROP FUNCTION wide_dec128;
DROP FUNCTION wide_dec256;
DROP FUNCTION wide_lc;
DROP FUNCTION wide_lc_rows;
EOF

# Rejections: the two LowCardinality shapes COLUMNAR_V1 cannot represent, refused at CREATE
# (separate calls: a failing statement must not abort the round-trip block above).
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 --query "
CREATE OR REPLACE FUNCTION wide_lc_nullable
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_lc_col'
    ARGUMENTS (s LowCardinality(Nullable(String))) RETURNS String DETERMINISTIC;" 2>&1 \
    | grep -o -m1 'does not support LowCardinality(Nullable(T)) arguments' | head -1
${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 --query "
CREATE OR REPLACE FUNCTION wide_lc_result
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_wide_abi' :: 'hex_lc_col'
    ARGUMENTS (s LowCardinality(String)) RETURNS LowCardinality(String) DETERMINISTIC;" 2>&1 \
    | grep -o -m1 'does not support a LowCardinality result type' | head -1

${CLICKHOUSE_CLIENT} --query "
DELETE FROM system.webassembly_modules WHERE name = 'columnar_wide_abi';"
