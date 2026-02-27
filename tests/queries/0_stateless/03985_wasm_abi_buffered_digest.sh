#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS digest_csv;
DROP FUNCTION IF EXISTS digest_tsv;
DROP FUNCTION IF EXISTS digest_json;
DROP FUNCTION IF EXISTS always_returns_ten_rows;
DROP FUNCTION IF EXISTS wasm_get_block_size15;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi';

EOF

cat ${CUR_DIR}/wasm/buffered_abi.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'buffered_abi', code FROM input('code String') FORMAT RawBlob"

# Verify that data is properly serialized and deserialized with ABI BUFFERED_V1 and memory layout is correct
# Guest will just calculate hash of the input data, so if serialization or memory layout is broken, the result will be different

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE OR REPLACE FUNCTION digest_csv
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'buffered_abi' :: 'digest_newline_rows'
    ARGUMENTS (s String, n UInt64, arr Array(Int64)) RETURNS UInt64
    SETTINGS serialization_format = 'CSV', max_fuel = 1_000_000;

CREATE OR REPLACE FUNCTION digest_tsv
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'buffered_abi' :: 'digest_newline_rows'
    ARGUMENTS (s String, n UInt64, arr Array(Int64)) RETURNS String
    SETTINGS serialization_format = 'TSV', max_fuel = 1_000_000;

CREATE OR REPLACE FUNCTION digest_json
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'buffered_abi' :: 'digest_json_rows'
    ARGUMENTS (s String, n UInt64, arr Array(Int64)) RETURNS Array(UInt64)
    SETTINGS serialization_format = 'JSONEachRow', max_fuel = 1_000_000;

DROP TABLE IF EXISTS test_data;
CREATE TABLE IF NOT EXISTS test_data (s String, n UInt64, arr Array(Int64)) ENGINE = Memory;
INSERT INTO test_data SELECT
    'val' || toString(number),
    sipHash64(number, 'a') :: UInt64,
    arrayMap(x -> sipHash64(number, 'b') % 100, range(10)) :: Array(Int64)
FROM numbers(1_000_00);

SELECT sum(digest_csv(s, n, arr)) FROM test_data;
SELECT groupArray(digest_tsv(s, n, arr)) FROM (SELECT * FROM test_data ORDER BY n LIMIT 5);
SELECT sum(arraySum(digest_json(s, n, arr))) FROM test_data;
SELECT digest_json(s, n, arr) FROM (SELECT * FROM test_data ORDER BY n LIMIT 2);
SELECT digest_json(a, b, c) FROM (SELECT s as a, n as b, arr as c FROM test_data ORDER BY n LIMIT 2);

CREATE OR REPLACE FUNCTION always_returns_ten_rows LANGUAGE WASM ABI BUFFERED_V1 FROM 'buffered_abi' ARGUMENTS (value UInt64) RETURNS UInt64 SETTINGS serialization_format  = 'CSV';
SELECT always_returns_ten_rows(number) FROM numbers(1); -- { serverError WASM_ERROR }

CREATE OR REPLACE FUNCTION wasm_get_block_size15 LANGUAGE WASM ABI BUFFERED_V1 FROM 'buffered_abi' :: 'get_block_size' ARGUMENTS (value UInt64) RETURNS UInt64 SETTINGS serialization_format  = 'CSV', max_input_block_size = 15;
SELECT min(wasm_get_block_size15(number) AS v), max(v) FROM numbers(10000) SETTINGS max_block_size = 65000;
SELECT min(wasm_get_block_size15(number) AS v), max(v) FROM numbers(8) SETTINGS max_block_size = 65000;

EOF
