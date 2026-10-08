-- Tags: no-fasttest
-- Test valid witness versions (0-16)
SELECT bech32Encode('bc', 'test', 0);
SELECT bech32Encode('bc', 'test', 1);
SELECT bech32Encode('bc', 'test', 16);

-- Test invalid witness versions (should throw BAD_ARGUMENTS exception)
SELECT bech32Encode('bc', 'test', 17); -- { serverError BAD_ARGUMENTS }
SELECT bech32Encode('bc', 'test', 32); -- { serverError BAD_ARGUMENTS }
SELECT bech32Encode('bc', 'test', 40); -- { serverError BAD_ARGUMENTS }
SELECT bech32Encode('bc', 'test', 255); -- { serverError BAD_ARGUMENTS }

-- Test the original fuzzer repro case (witness version 40 causing buffer overflow)
DROP TABLE IF EXISTS hex_data_test;
SET allow_suspicious_low_cardinality_types=1;
CREATE TABLE hex_data_test (hrp String, data String, witver LowCardinality(UInt8)) ENGINE = Memory;
INSERT INTO hex_data_test VALUES ('bc', 'test_data', 0);
SELECT bech32Encode(hrp, toFixedString('751e76e8199196d454941c45d1b3a323f1433bd6', 40), 40) FROM hex_data_test; -- { serverError BAD_ARGUMENTS }
DROP TABLE hex_data_test;
