-- Tags: no-fasttest, no-parallel, no-msan

-- Checks general functionality of managing WebAssembly modules and functions (adding modules, creating functions, etc)
-- And basic wasm engine functionality, such as calling simple functions

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS wasm_data;

CREATE TABLE wasm_data (s String) ENGINE = Memory;
-- https://github.com/eliben/wasm-wat-samples/blob/125f27fa4bf7fb3c5aab5479136be4596150420a/prime-test/isprime.wat
INSERT INTO wasm_data SELECT concat(
    'AGFzbQEAAAABBgFgAX8BfwMCAQAHDAEIaXNfcHJpbWUAAApUAVIBAX8gAEECSQRAQQAPCyAAQQJG',
    'BEBBAQ8LIABBAnBBAEYEQEEADwtBAyEBA0ACQCABIABPDQAgACABcEEARgRAQQAPCyABQQJqIQEM',
    'AQsLQQEL');

DROP FUNCTION IF EXISTS is_prime;
DELETE FROM system.webassembly_modules WHERE name = 'module1';

INSERT INTO system.webassembly_modules (name) SELECT 'module1'; -- { serverError INCORRECT_DATA }
INSERT INTO system.webassembly_modules (name, code) SELECT 'module1', repeat('a', 1000); -- { serverError INCORRECT_DATA }
INSERT INTO system.webassembly_modules (code) SELECT base64Decode(s) FROM wasm_data; -- { serverError INCORRECT_DATA }
INSERT INTO system.webassembly_modules (name, code) SELECT repeat('a', 1000), base64Decode(s) FROM wasm_data; -- { serverError INCORRECT_DATA }

-- digest mismatch
INSERT INTO system.webassembly_modules (name, code, hash)
    SELECT
        'module1',
        base64Decode(s),
        reinterpretAsUInt256(unhex('baaaaaaaad1ac69a97735c26039f090ab78c31d729e8110f086b2ea222222222'))
    FROM wasm_data; -- { serverError INCORRECT_DATA }

INSERT INTO system.webassembly_modules (name, code, hash)
    SELECT
        'module1',
        base64Decode(s),
        reinterpretAsUInt256(unhex('369f6098ed1ac69a97735c26039f090ab78c31d729e8110f086b2ea13611c57d'))
    FROM wasm_data;

SELECT name, code == '' FROM system.webassembly_modules WHERE name = 'module1';

-- inserting existing module
-- idempotent
INSERT INTO system.webassembly_modules (name, code) SELECT 'module1', base64Decode(s) FROM wasm_data;
-- replace is not allowed
INSERT INTO system.webassembly_modules (name, code) SELECT 'module1', base64Decode('AGFzbQEAAAABBwFgAn9/AX8DAgEABwcBA2FkZAAACgkBBwAgACABags=') FROM wasm_data; -- { serverError FILE_ALREADY_EXISTS }

DELETE FROM system.webassembly_modules WHERE name = 'module1';
INSERT INTO system.webassembly_modules (name, code) SELECT 'module1', base64Decode('AGFzbQEAAAABBwFgAn9/AX8DAgEABwcBA2FkZAAACgkBBwAgACABags=') FROM wasm_data;
DELETE FROM system.webassembly_modules WHERE name = 'module1';
INSERT INTO system.webassembly_modules (name, code) SELECT 'module1', base64Decode(s) FROM wasm_data;

-- wrong module name
CREATE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT FROM 'unknown_module' ARGUMENTS (num UInt32) RETURNS UInt32; -- { serverError RESOURCE_NOT_FOUND }
-- wrong function name
CREATE FUNCTION unknown_function LANGUAGE WASM ABI ROW_DIRECT FROM 'module1' ARGUMENTS (num UInt32) RETURNS UInt32; -- { serverError BAD_ARGUMENTS }
-- wrong argument type
CREATE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT FROM 'module1' ARGUMENTS (num UInt64) RETURNS UInt64; -- { serverError BAD_ARGUMENTS }

CREATE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT FROM 'module1' ARGUMENTS (num UInt32) RETURNS UInt32;

SELECT name, lower(hex(reinterpretAsFixedString(hash))) FROM system.webassembly_modules WHERE name = 'module1';

-- Hash is added to create query
SELECT name, create_query LIKE '%369f6098ed1ac69a97735c26039f090ab78c31d729e8110f086b2ea13611c57d%' FROM system.functions WHERE name = 'is_prime';

SELECT is_prime('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT is_prime(1 :: UInt64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT is_prime(1 :: UInt32, 2 :: UInt32); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT number FROM numbers(2, 10) WHERE (is_prime(number :: UInt32) :: Bool);

-- check that sql and wasm functions correctly works in case of name collision
CREATE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT FROM 'module1' ARGUMENTS (num UInt32) RETURNS UInt32; -- { serverError FUNCTION_ALREADY_EXISTS }
CREATE FUNCTION is_prime AS x -> x + 1; -- { serverError FUNCTION_ALREADY_EXISTS }

DROP FUNCTION is_prime;
CREATE FUNCTION is_prime AS x -> x + 1;

SELECT 'wrong', name, is_prime(7717 :: UInt32) FROM system.functions WHERE name = 'is_prime';
CREATE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT FROM 'module1' ARGUMENTS (num UInt32) RETURNS UInt32; -- { serverError FUNCTION_ALREADY_EXISTS }

CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT FROM 'module1' ARGUMENTS (UInt32) RETURNS UInt32;
SELECT 'correct', name, is_prime(7717 :: UInt32) FROM system.functions WHERE name = 'is_prime';

-- OR REPLACE is not stored in create query
SELECT name, create_query like 'CREATE FUNCTION is_prime LANGUAGE WASM %' FROM system.functions WHERE name = 'is_prime';

-- source function name is wrong
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM FROM 'module1' :: 'unknown_is_prime' RETURNS UInt32 ARGUMENTS (num UInt32); -- { serverError BAD_ARGUMENTS }
-- source function name is case-sensitive
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM FROM 'module1' :: 'Is_Prime' RETURNS UInt32 ARGUMENTS (num UInt32); -- { serverError BAD_ARGUMENTS }
CREATE OR REPLACE FUNCTION Is_Prime LANGUAGE WASM FROM 'module1' RETURNS UInt32 ARGUMENTS (num UInt32); -- { serverError BAD_ARGUMENTS }
-- unknown ABI
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ARGUMENTS (num UInt32) RETURNS UInt32 FROM 'module1' ABI UNKNOWN; -- { serverError BAD_ARGUMENTS }

-- no RETURNs
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM FROM 'module1' ARGUMENTS (num UInt32); -- { clientError SYNTAX_ERROR }
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ARGUMENTS UInt32 RETURNS UInt32 FROM 'module1' ABI ROW_DIRECT; -- { clientError SYNTAX_ERROR }
-- multivalue RETURNS
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT FROM 'module1' ARGUMENTS (UInt32) RETURNS (UInt32, UInt32); -- { clientError SYNTAX_ERROR }
-- LANGUAGE is missing
CREATE OR REPLACE FUNCTION is_prime WASM ARGUMENTS (num UInt32) RETURNS UInt32 FROM 'module1' ABI ROW_DIRECT; -- { clientError SYNTAX_ERROR }
-- LANGUAGE is wrong
CREATE OR REPLACE FUNCTION is_prime LANGUAGE JS ABI ROW_DIRECT FROM 'module1' ARGUMENTS (UInt32) RETURNS UInt32; -- { clientError SYNTAX_ERROR }
-- muplitple ABI keywords
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT FROM 'module1' ARGUMENTS (UInt32) RETURNS UInt32 ABI ROW_DIRECT; -- { clientError SYNTAX_ERROR }
-- ABI is identifier, not a string literal
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ARGUMENTS (num UInt32) RETURNS UInt32 FROM 'module1' ABI 'PLAIN'; -- { clientError SYNTAX_ERROR }
-- RETURNS with column name is not allowed
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ARGUMENTS (num UInt32) RETURNS (x UInt32) FROM 'module1' ABI ROW_DIRECT; -- { clientError SYNTAX_ERROR }
-- illegal literal for module name
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ARGUMENTS (num UInt32) RETURNS UInt32 FROM 123 ABI ROW_DIRECT; -- { clientError SYNTAX_ERROR }

CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ARGUMENTS (num UInt32) RETURNS UInt32 FROM 'module1' ABI ROW_DIRECT;
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM ARGUMENTS (num UInt32) RETURNS UInt32 FROM 'module1';
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM FROM 'module1' :: 'is_prime' RETURNS UInt32 ARGUMENTS (num UInt32);
CREATE OR REPLACE FUNCTION is_prime LANGUAGE WASM SHA256_HASH 'cafebabe' FROM 'module1' RETURNS UInt32 ARGUMENTS (num UInt32); -- { serverError BAD_ARGUMENTS }
SELECT 'correct', name, is_prime(13331 :: UInt32) FROM system.functions WHERE name = 'is_prime';

-- cannot drop or replace module because function is still in use
DELETE FROM system.webassembly_modules WHERE name = 'module1'; -- { serverError CANNOT_DROP_FUNCTION }
INSERT INTO system.webassembly_modules (name, code) SELECT 'module1', base64Decode('AGFzbQEAAAABBwFgAn9/AX8DAgEABwcBA2FkZAAACgkBBwAgACABags=') FROM wasm_data; -- { serverError FILE_ALREADY_EXISTS }

CREATE OR REPLACE FUNCTION is_prime AS x -> x + 1;

DELETE FROM system.webassembly_modules WHERE name like 'module1'; -- { serverError BAD_ARGUMENTS }
DELETE FROM system.webassembly_modules WHERE code == '123'; -- { serverError BAD_ARGUMENTS }
DELETE FROM system.webassembly_modules WHERE hash == 1; -- { serverError BAD_ARGUMENTS }

DELETE FROM system.webassembly_modules WHERE name = 'module1';

DROP FUNCTION is_prime;
DROP FUNCTION IF EXISTS is_prime;

DROP FUNCTION IF EXISTS return_default;
DROP FUNCTION IF EXISTS times2;
DELETE FROM system.webassembly_modules WHERE name = 'wasm_s';
DELETE FROM system.webassembly_modules WHERE name = 'wasm_l';
DELETE FROM system.webassembly_modules WHERE name = 'wasm_h';
DELETE FROM system.webassembly_modules WHERE name = 'wasm_h2';

INSERT INTO system.webassembly_modules (name, code) SELECT 'wasm_l', base64Decode(concat(
        'AGFzbQEAAAABFgNgAAF/YAJ/fwN/f39gAn9/BH9/f38DBgUAAQECAgdQBQ5yZXR1cm5fZGVmYXVs',
        'dAAADnVubmFtZWRfbG9jYWxzAAEObmFtZWRfYnlfaW5kZXgAAgxzb21lX3VubmFtZWQAAwptdWx0',
        'aV9kZWNsAAQKUgUGAQF/IAALDgEBf0EsIQIgACABIAILDwEBf0HYACECIAAgASACCxUBAn9BzQAh',
        'AkEsIQMgACABIAIgAwsUAQJ/QSEhAkEsIQMgACABIAIgAws='));

SELECT name FROM system.webassembly_modules WHERE name LIKE 'wasm_%' ORDER BY name;

CREATE FUNCTION multi_decl LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_l' ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32; -- { serverError WASM_ERROR }

SELECT * FROM system.functions WHERE name = 'multi_decl';

CREATE FUNCTION return_default LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_l' ARGUMENTS () RETURNS Int32;
SELECT name, return_default() == 0 FROM system.functions WHERE name = 'return_default';
DROP FUNCTION return_default;

-- module with zeros in the beginning or end of the hash, check that it's not trimmed
INSERT INTO system.webassembly_modules (name, code, hash) SELECT * FROM (
    SELECT 'wasm_h', base64Decode(concat(
        'AGFzbQEAAAABBgFgAX8BfwMCAQAHCgEGdGltZXMyAAAKCQEHACAAIABqCwAsBmN1c3RvbTZHVFZS',
        'dDJ4Tkk4dCl6WDdsYS9weD08ajVKbmg1R3FkcmtEXWc=')),
        reinterpretAsUInt256(unhex('0000049174a04c6321c4740b0f4fea5b1a387b38cb56b94bbea533868675e53a'))
    UNION ALL
    SELECT 'wasm_h2', base64Decode(concat(
        'AGFzbQEAAAABBgFgAX8BfwMCAQAHCgEGdGltZXMyAAAKCQEHACAAIABqCwAMBmN1c3RvbVNyN1Bj')),
        reinterpretAsUInt256(unhex('79b1fd5fb2e3acb6ca11b436ab354a622d726085effded25cd0ac3046e300000'))
);

CREATE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '49174a04c6321c4740b0f4fea5b1a387b38cb56b94bbea533868675e53a'; -- { serverError BAD_ARGUMENTS }
CREATE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '049174a04c6321c4740b0f4fea5b1a387b38cb56b94bbea533868675e53a'; -- { serverError BAD_ARGUMENTS }
CREATE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '00049174a04c6321c4740b0f4fea5b1a387b38cb56b94bbea533868675e53a'; -- { serverError BAD_ARGUMENTS }
CREATE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '1000049174a04c6321c4740b0f4fea5b1a387b38cb56b94bbea533868675e53a'; -- { serverError BAD_ARGUMENTS }
CREATE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '0000049174a04c6321c4740b0f4fea5b1a387b38cb56b94bbea533868675e53a';
SELECT name, create_query LIKE '%\'0000049174a04c6321c4740b0f4fea5b1a387b38cb56b94bbea533868675e53a\'%' FROM system.functions WHERE name = 'times2';
SELECT name, lower(hex(reinterpretAsFixedString(hash))) LIKE '000004917%' FROM system.webassembly_modules WHERE name = 'wasm_h';
SELECT times2(21 :: Int32);

CREATE OR REPLACE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h2' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '79b1fd5fb2e3acb6ca11b436ab354a622d726085effded25cd0ac3046e3'; -- { serverError BAD_ARGUMENTS }
CREATE OR REPLACE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h2' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '79b1fd5fb2e3acb6ca11b436ab354a622d726085effded25cd0ac3046e30'; -- { serverError BAD_ARGUMENTS }
CREATE OR REPLACE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h2' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '79b1fd5fb2e3acb6ca11b436ab354a622d726085effded25cd0ac3046e3000'; -- { serverError BAD_ARGUMENTS }
CREATE OR REPLACE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h2' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '79b1fd5fb2e3acb6ca11b436ab354a622d726085effded25cd0ac3046e300001'; -- { serverError BAD_ARGUMENTS }
CREATE OR REPLACE FUNCTION times2 LANGUAGE WASM ABI ROW_DIRECT FROM 'wasm_h2' ARGUMENTS (Int32) RETURNS Int32 SHA256_HASH '79b1fd5fb2e3acb6ca11b436ab354a622d726085effded25cd0ac3046e300000';
SELECT name, create_query LIKE '%\'79b1fd5fb2e3acb6ca11b436ab354a622d726085effded25cd0ac3046e300000\'%' FROM system.functions WHERE name = 'times2';
SELECT name, lower(hex(reinterpretAsFixedString(hash))) LIKE '%46e300000' FROM system.webassembly_modules WHERE name = 'wasm_h2';

SELECT times2(21 :: Int32);

DROP FUNCTION times2;

DELETE FROM system.webassembly_modules WHERE name = 'wasm_s';
DELETE FROM system.webassembly_modules WHERE name = 'wasm_l';
DELETE FROM system.webassembly_modules WHERE name = 'wasm_h';
DELETE FROM system.webassembly_modules WHERE name = 'wasm_h2';
