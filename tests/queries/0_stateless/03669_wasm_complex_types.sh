#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS add3_i64;
DROP FUNCTION IF EXISTS add3_i32;
DROP FUNCTION IF EXISTS add3_f32;
DROP FUNCTION IF EXISTS add3_f64;
DROP FUNCTION IF EXISTS vadd3_u64;
DROP FUNCTION IF EXISTS golden_ratio;
DROP FUNCTION IF EXISTS simple_counter;
DROP FUNCTION IF EXISTS complex_data_type;
DROP FUNCTION IF EXISTS parse_pem;
DROP FUNCTION IF EXISTS always_returns_ten_rows;
DROP FUNCTION IF EXISTS wasm_get_block_size2;
DROP FUNCTION IF EXISTS wasm_get_block_size15;
DROP FUNCTION IF EXISTS hello_csv;
DROP FUNCTION IF EXISTS hello_csv_wrong;
DROP FUNCTION IF EXISTS add3_csv;

DELETE FROM system.webassembly_modules WHERE name = 'clickhouse_wasm_example';

EOF

cat ${CUR_DIR}/wasm/clickhouse_wasm_example.wasm.gz | gunzip | ${CLICKHOUSE_CLIENT} --query "
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT
    'clickhouse_wasm_example' AS name,
    raw_blob AS code,
    toUInt256('109512041981872266840431839764139570488964022093810554932159840724338894707416') as hash
FROM input('raw_blob String') FORMAT RawBlob
"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE OR REPLACE FUNCTION add3_i64 LANGUAGE WASM ABI PLAIN FROM 'clickhouse_wasm_example' ARGUMENTS (UInt64, UInt64, UInt64) RETURNS UInt64;
CREATE OR REPLACE FUNCTION add3_i32 LANGUAGE WASM ABI PLAIN FROM 'clickhouse_wasm_example' ARGUMENTS (UInt32, UInt32, UInt32) RETURNS UInt32;
CREATE OR REPLACE FUNCTION add3_f32 LANGUAGE WASM ABI PLAIN FROM 'clickhouse_wasm_example' ARGUMENTS (Float32, Float32, Float32) RETURNS Float32;
CREATE OR REPLACE FUNCTION add3_f64 LANGUAGE WASM ABI PLAIN FROM 'clickhouse_wasm_example' ARGUMENTS (Float64, Float64, Float64) RETURNS Float64;
CREATE OR REPLACE FUNCTION vadd3_u64 LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'clickhouse_wasm_example' ARGUMENTS (UInt64, UInt64, UInt64) RETURNS UInt64;
CREATE OR REPLACE FUNCTION add3_csv LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'clickhouse_wasm_example' ARGUMENTS (UInt64, UInt64, UInt64) RETURNS UInt64 SETTINGS serialization_format  = 'CSV';

SELECT
    number AS a,
    2 AS b,
    materialize(3) AS c,
    a + b + c AS answer,
    vadd3_u64(a :: UInt64, b :: UInt64, c :: UInt64) == answer,
    add3_csv(a :: UInt64, b :: UInt64, c :: UInt64) == answer,
    add3_i64(a :: UInt64, b :: UInt64, c :: UInt64) == answer,
    add3_i32(a :: UInt32, b :: UInt32, c :: UInt32) == answer,
    abs(add3_f32(a :: Float32, b :: Float32, c :: Float32) - answer) < 1e-6,
    abs(add3_f64(a :: Float64, b :: Float64, c :: Float64) - answer) < 1e-6,
FROM numbers(10)
ORDER BY number
;

CREATE OR REPLACE FUNCTION golden_ratio LANGUAGE WASM ABI PLAIN FROM 'clickhouse_wasm_example' ARGUMENTS () RETURNS Float64;
SELECT round(golden_ratio(), 3) AS golden_ratio;

CREATE OR REPLACE FUNCTION simple_counter LANGUAGE WASM ABI PLAIN FROM 'clickhouse_wasm_example' :: 'simple_counter' ARGUMENTS () RETURNS UInt64 SETTINGS max_instances = 1;
SELECT simple_counter() AS val from numbers_mt(4) ORDER BY ALL SETTINGS max_block_size = 2;

CREATE OR REPLACE FUNCTION complex_data_type LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'clickhouse_wasm_example' ARGUMENTS (Map(String, UInt64), Array(String)) RETURNS UInt64;

SELECT complex_data_type(map('a', 20 :: UInt64, 'b', 100 :: UInt64, 'c', 20 + number :: UInt64), ['a', 'c']) FROM numbers(4);

CREATE OR REPLACE FUNCTION always_returns_ten_rows LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'clickhouse_wasm_example' ARGUMENTS (value UInt64) RETURNS UInt64 SETTINGS serialization_format  = 'CSV';
SELECT always_returns_ten_rows(number) FROM numbers(1); -- { serverError WASM_ERROR }

CREATE OR REPLACE FUNCTION hello_csv LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'clickhouse_wasm_example' ARGUMENTS () RETURNS Nullable(String) SETTINGS serialization_format  = 'CSV';
SELECT hello_csv() like 'Hello, ClickHouse 2%!' FROM numbers(2);

CREATE OR REPLACE FUNCTION hello_csv_wrong LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'clickhouse_wasm_example' :: 'hello_csv' ARGUMENTS () RETURNS UInt64 SETTINGS serialization_format  = 'CSV';
SELECT hello_csv_wrong() FROM numbers(1); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

CREATE OR REPLACE FUNCTION wasm_get_block_size2 LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'clickhouse_wasm_example' :: 'get_block_size' ARGUMENTS (value UInt64) RETURNS UInt64 SETTINGS serialization_format  = 'CSV', max_input_block_size = 2;
SELECT min(wasm_get_block_size2(number) AS v), max(v) FROM numbers(10000) SETTINGS max_block_size = 65000;

CREATE OR REPLACE FUNCTION wasm_get_block_size15 LANGUAGE WASM ABI UNSTABLE_V0_1 FROM 'clickhouse_wasm_example' :: 'get_block_size' ARGUMENTS (value UInt64) RETURNS UInt64 SETTINGS serialization_format  = 'CSV', max_input_block_size = 15;
SELECT min(wasm_get_block_size15(number) AS v), max(v) FROM numbers(10000) SETTINGS max_block_size = 65000;
SELECT min(wasm_get_block_size15(number) AS v), max(v) FROM numbers(8) SETTINGS max_block_size = 65000;

CREATE OR REPLACE FUNCTION parse_pem LANGUAGE WASM ABI unstable_v0_1 FROM 'clickhouse_wasm_example' ARGUMENTS (s String) RETURNS Map(String, String);
SELECT
    m['subject'] as subject,
    toDateTime(toUInt64OrZero(m['not_before']), 'Europe/London') as not_before,
    toDateTime(toUInt64OrZero(m['not_after']), 'Europe/London') as not_after
FROM (
    -- Command to get certificate: "openssl s_client -connect clickhouse.com:443 </dev/null | openssl x509 -outform PEM"
    SELECT parse_pem(concat(
        '-----BEGIN CERTIFICATE-----', '\n',
        'MIIDsDCCA1agAwIBAgIRAKs4Xe4kUlfQDWpx0zRcEqwwCgYIKoZIzj0EAwIwOzELMAkGA1UEBhMCVVMxHjAcBgNVBAoTFUdvb2dsZSBUcnVzdCBTZXJ2aWNlczEMMAoG',
        'A1UEAxMDV0UxMB4XDTI0MDgyOTEwMTIxM1oXDTI0MTEyNzEwNDUzMlowGTEXMBUGA1UEAxMOY2xpY2tob3VzZS5jb20wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQR',
        'O7WGBt5RbM4SpNB/C5Xx/j2R1mPGgyeYlgoJ4DXHRV2ucoYN2c6fv//PyxaoZuN2qH66hISD0SlzNq6PhzEbo4ICWzCCAlcwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQM',
        'MAoGCCsGAQUFBwMBMAwGA1UdEwEB/wQCMAAwHQYDVR0OBBYEFKo3LRQUsE3ZTfHrjEQy8miAswyOMB8GA1UdIwQYMBaAFJB3kjVnxP+ozKnme9mAeXvMk/k4MF4GCCsG',
        'AQUFBwEBBFIwUDAnBggrBgEFBQcwAYYbaHR0cDovL28ucGtpLmdvb2cvcy93ZTEvcXpnMCUGCCsGAQUFBzAChhlodHRwOi8vaS5wa2kuZ29vZy93ZTEuY3J0MC4GA1Ud',
        'EQQnMCWCDmNsaWNraG91c2UuY29tghNkYXRhLmNsaWNraG91c2UuY29tMBMGA1UdIAQMMAowCAYGZ4EMAQIBMDYGA1UdHwQvMC0wK6ApoCeGJWh0dHA6Ly9jLnBraS5n',
        'b29nL3dlMS9sSXR3RTdGSE9PTS5jcmwwggEDBgorBgEEAdZ5AgQCBIH0BIHxAO8AdQDuzdBk1dsazsVct520zROiModGfLzs3sNRSFlGcR+1mwAAAZGd1gQsAAAEAwBG',
        'MEQCIGhzkQ7fKdn781DmIfVvW36UAVSpzylnI1spVrXF4QetAiBPQ0YX7ok04aAIjyxhJOS3A2vANTk3myDLQgXw+xFWxwB2ANq2v2s/tbYin5vCu1xr6HCRcWy7UYSF',
        'NL2kPTBI1/urAAABkZ3WBGMAAAQDAEcwRQIhALqJt9Y33HXkwMxa6vBjPtb1hFt8bnih8I4MVQwssw63AiAkUsBgPRbvdVdUsYL6TG2OLDbqsYDWam01ktFpQQoaSjAK',
        'BggqhkjOPQQDAgNIADBFAiBH4WNNpPaoCt+K8zcafamSvtMKmg7bJg+2T/Cs4+zC9wIhAK+yb2CTZOKxsMUkP6s3QW7tKBLC0BWMIgmO4iGRssZ2', '\n',
        '-----END CERTIFICATE-----')) as m
);

EOF
