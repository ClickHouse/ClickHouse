-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

--- aes_encrypt_mysql(string, key, block_mode[, init_vector, AAD])
-- The MySQL-compatitable encryption, only ecb, cbc, cfb128 and ofb modes are supported,
-- just like for MySQL
-- https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-encrypt
-- https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_block_encryption_mode
-- Please note that for keys that exceed mode-specific length, keys are folded in a MySQL-specific way,
-- meaning that whole key is used, but effective key length is still determined by mode.
-- when key doesn't exceed the default mode length, ecryption result equals with AES_encypt()

-----------------------------------------------------------------------------------------
-- error cases
-----------------------------------------------------------------------------------------
SELECT aes_encrypt_mysql(); --{serverError 42} not enough arguments
SELECT aes_encrypt_mysql('aes-128-ecb'); --{serverError 42} not enough arguments
SELECT aes_encrypt_mysql('aes-128-ecb', 'text'); --{serverError 42} not enough arguments

-- Mode
SELECT aes_encrypt_mysql(789, 'text', 'key'); --{serverError 43} bad mode type
SELECT aes_encrypt_mysql('blah blah blah', 'text', 'key'); -- {serverError 36} garbage mode value
SELECT aes_encrypt_mysql('des-ede3-ecb', 'text', 'key'); -- {serverError 36} bad mode value of valid cipher name
SELECT aes_encrypt_mysql('aes-128-gcm', 'text', 'key'); -- {serverError 36} mode is not supported by _mysql-functions

SELECT encrypt(789, 'text', 'key'); --{serverError 43} bad mode type
SELECT encrypt('blah blah blah', 'text', 'key'); -- {serverError 36} garbage mode value
SELECT encrypt('des-ede3-ecb', 'text', 'key'); -- {serverError 36} bad mode value of valid cipher name


-- Key
SELECT aes_encrypt_mysql('aes-128-ecb', 'text', 456); --{serverError 43} bad key type
SELECT aes_encrypt_mysql('aes-128-ecb', 'text', 'key'); -- {serverError 36} key is too short

SELECT encrypt('aes-128-ecb', 'text'); --{serverError 42} key is missing
SELECT encrypt('aes-128-ecb', 'text', 456); --{serverError 43} bad key type
SELECT encrypt('aes-128-ecb', 'text', 'key'); -- {serverError 36} key is too short
SELECT encrypt('aes-128-ecb', 'text', 'keykeykeykeykeykeykeykeykeykeykeykey'); -- {serverError 36} key is to long

-- IV
SELECT aes_encrypt_mysql('aes-128-ecb', 'text', 'key', 1011); --{serverError 43} bad IV type 6
SELECT aes_encrypt_mysql('aes-128-ecb', 'text', 'key', 'iv'); --{serverError 36} IV is too short 4

SELECT encrypt('aes-128-cbc', 'text', 'keykeykeykeykeyk', 1011); --{serverError 43} bad IV type 1
SELECT encrypt('aes-128-cbc', 'text', 'keykeykeykeykeyk', 'iviviviviviviviviviviviviviviviviviviviviv'); --{serverError 36} IV is too long 3
SELECT encrypt('aes-128-cbc', 'text', 'keykeykeykeykeyk', 'iv'); --{serverError 36} IV is too short 2

--AAD
SELECT aes_encrypt_mysql('aes-128-ecb', 'text', 'key', 'IV', 1213); --{serverError 42} too many arguments

SELECT encrypt('aes-128-ecb', 'text', 'key', 'IV', 1213); --{serverError 43} bad AAD type
SELECT encrypt('aes-128-gcm', 'text', 'key', 'IV', 1213); --{serverError 43} bad AAD type

-----------------------------------------------------------------------------------------
-- Validate against predefined ciphertext,plaintext,key and IV for MySQL compatibility mode
-----------------------------------------------------------------------------------------
CREATE TABLE encryption_test
(
    input String,
    key String DEFAULT unhex('fb9958e2e897ef3fdb49067b51a24af645b3626eed2f9ea1dc7fd4dd71b7e38f9a68db2a3184f952382c783785f9d77bf923577108a88adaacae5c141b1576b0'),
    iv String DEFAULT unhex('8CA3554377DFF8A369BC50A89780DD85'),
    key32 String DEFAULT substring(key, 1, 32),
    key24 String DEFAULT substring(key, 1, 24),
    key16 String DEFAULT substring(key, 1, 16)
) Engine = Memory;

INSERT INTO encryption_test (input)
VALUES (''), ('text'), ('What Is ClickHouse? ClickHouse is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).');


SELECT 'MySQL-compatitable mode, with key folding, no length checks, etc.';
SELECT 'aes-128-cbc' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
SELECT 'aes-192-cbc' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
SELECT 'aes-256-cbc' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;

SELECT 'aes-128-cfb128' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
SELECT 'aes-192-cfb128' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
SELECT 'aes-256-cfb128' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;

SELECT 'aes-128-ecb' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
SELECT 'aes-192-ecb' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
SELECT 'aes-256-ecb' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;

SELECT 'aes-128-ofb' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
SELECT 'aes-192-ofb' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
SELECT 'aes-256-ofb' as mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;


SELECT 'Strict mode without key folding and proper key and iv lengths checks.';
SELECT 'aes-128-cbc' as mode, hex(encrypt(mode, input, key16, iv)) FROM encryption_test;
SELECT 'aes-192-cbc' as mode, hex(encrypt(mode, input, key24, iv)) FROM encryption_test;
SELECT 'aes-256-cbc' as mode, hex(encrypt(mode, input, key32, iv)) FROM encryption_test;

SELECT 'aes-128-cfb128' as mode, hex(encrypt(mode, input, key16, iv)) FROM encryption_test;
SELECT 'aes-192-cfb128' as mode, hex(encrypt(mode, input, key24, iv)) FROM encryption_test;
SELECT 'aes-256-cfb128' as mode, hex(encrypt(mode, input, key32, iv)) FROM encryption_test;

SELECT 'aes-128-ctr' as mode, hex(encrypt(mode, input, key16, iv)) FROM encryption_test;
SELECT 'aes-192-ctr' as mode, hex(encrypt(mode, input, key24, iv)) FROM encryption_test;
SELECT 'aes-256-ctr' as mode, hex(encrypt(mode, input, key32, iv)) FROM encryption_test;

SELECT 'aes-128-ecb' as mode, hex(encrypt(mode, input, key16)) FROM encryption_test;
SELECT 'aes-192-ecb' as mode, hex(encrypt(mode, input, key24)) FROM encryption_test;
SELECT 'aes-256-ecb' as mode, hex(encrypt(mode, input, key32)) FROM encryption_test;

SELECT 'aes-128-ofb' as mode, hex(encrypt(mode, input, key16, iv)) FROM encryption_test;
SELECT 'aes-192-ofb' as mode, hex(encrypt(mode, input, key24, iv)) FROM encryption_test;
SELECT 'aes-256-ofb' as mode, hex(encrypt(mode, input, key32, iv)) FROM encryption_test;

SELECT 'GCM mode with IV';
SELECT 'aes-128-gcm' as mode, hex(encrypt(mode, input, key16, iv)) FROM encryption_test;
SELECT 'aes-192-gcm' as mode, hex(encrypt(mode, input, key24, iv)) FROM encryption_test;
SELECT 'aes-256-gcm' as mode, hex(encrypt(mode, input, key32, iv)) FROM encryption_test;

SELECT 'GCM mode with IV and AAD';
SELECT 'aes-128-gcm' as mode, hex(encrypt(mode, input, key16, iv, 'AAD')) FROM encryption_test;
SELECT 'aes-192-gcm' as mode, hex(encrypt(mode, input, key24, iv, 'AAD')) FROM encryption_test;
SELECT 'aes-256-gcm' as mode, hex(encrypt(mode, input, key32, iv, 'AAD')) FROM encryption_test;

SELECT 'Nullable and LowCardinality';
WITH CAST(NULL as Nullable(String)) as input, 'aes-256-ofb' as mode SELECT toTypeName(input), hex(aes_encrypt_mysql(mode, input, key32,iv)) FROM encryption_test LIMIT 1;
WITH CAST('text' as Nullable(String)) as input, 'aes-256-ofb' as mode SELECT toTypeName(input), hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test LIMIT 1;
WITH CAST('text' as LowCardinality(String)) as input, 'aes-256-ofb' as mode SELECT toTypeName(input), hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test LIMIT 1;

-- based on https://github.com/openssl/openssl/blob/master/demos/evp/aesgcm.c#L20
WITH
    unhex('eebc1f57487f51921c0465665f8ae6d1658bb26de6f8a069a3520293a572078f') as key,
    unhex('67ba0510262ae487d737ee6298f77e0c') as tag,
    unhex('99aa3e68ed8173a0eed06684') as iv,
    unhex('f56e87055bc32d0eeb31b2eacc2bf2a5') as plaintext,
    unhex('4d23c3cec334b49bdb370c437fec78de') as aad,
    unhex('f7264413a84c0e7cd536867eb9f21736') as ciphertext
SELECT
    hex(encrypt('aes-256-gcm', plaintext, key, iv, aad)) as ciphertext_actual,
    ciphertext_actual = concat(hex(ciphertext), hex(tag));

DROP TABLE encryption_test;
