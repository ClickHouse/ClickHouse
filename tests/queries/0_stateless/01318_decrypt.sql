-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

--- aes_decrypt_mysql(string, key, block_mode[, init_vector, AAD])
-- The MySQL-compatitable encryption, only ecb, cbc and ofb modes are supported,
-- just like for MySQL
-- https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-encrypt
-- https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_block_encryption_mode
-- Please note that for keys that exceed mode-specific length, keys are folded in a MySQL-specific way,
-- meaning that whole key is used, but effective key length is still determined by mode.
-- when key doesn't exceed the default mode length, ecryption result equals with AES_encypt()

-----------------------------------------------------------------------------------------
-- error cases
-----------------------------------------------------------------------------------------
SELECT aes_decrypt_mysql(); --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH} not enough arguments
SELECT aes_decrypt_mysql('aes-128-ecb'); --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH} not enough arguments
SELECT aes_decrypt_mysql('aes-128-ecb', 'text'); --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH} not enough arguments

-- Mode
SELECT aes_decrypt_mysql(789, 'text', 'key'); --{serverError ILLEGAL_TYPE_OF_ARGUMENT} bad mode type
SELECT aes_decrypt_mysql('blah blah blah', 'text', 'key'); -- {serverError BAD_ARGUMENTS} garbage mode value
SELECT aes_decrypt_mysql('des-ede3-ecb', 'text', 'key'); -- {serverError BAD_ARGUMENTS} bad mode value of valid cipher name
SELECT aes_decrypt_mysql('aes-128-gcm', 'text', 'key'); -- {serverError BAD_ARGUMENTS} mode is not supported by _mysql-functions

SELECT decrypt(789, 'text', 'key'); --{serverError ILLEGAL_TYPE_OF_ARGUMENT} bad mode type
SELECT decrypt('blah blah blah', 'text', 'key'); -- {serverError BAD_ARGUMENTS} garbage mode value
SELECT decrypt('des-ede3-ecb', 'text', 'key'); -- {serverError BAD_ARGUMENTS} bad mode value of valid cipher name


-- Key
SELECT aes_decrypt_mysql('aes-128-ecb', 'text', 456); --{serverError ILLEGAL_TYPE_OF_ARGUMENT} bad key type
SELECT aes_decrypt_mysql('aes-128-ecb', 'text', 'key'); -- {serverError BAD_ARGUMENTS} key is too short

SELECT decrypt('aes-128-ecb', 'text'); --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH} key is missing
SELECT decrypt('aes-128-ecb', 'text', 456); --{serverError ILLEGAL_TYPE_OF_ARGUMENT} bad key type
SELECT decrypt('aes-128-ecb', 'text', 'key'); -- {serverError BAD_ARGUMENTS} key is too short
SELECT decrypt('aes-128-ecb', 'text', 'keykeykeykeykeykeykeykeykeykeykeykey'); -- {serverError BAD_ARGUMENTS} key is to long

-- IV
SELECT aes_decrypt_mysql('aes-128-ecb', 'text', 'key', 1011); --{serverError ILLEGAL_TYPE_OF_ARGUMENT} bad IV type 6
SELECT aes_decrypt_mysql('aes-128-ecb', 'text', 'key', 'iv'); --{serverError BAD_ARGUMENTS} IV is too short 4

SELECT decrypt('aes-128-cbc', 'text', 'keykeykeykeykeyk', 1011); --{serverError ILLEGAL_TYPE_OF_ARGUMENT} bad IV type 1
SELECT decrypt('aes-128-cbc', 'text', 'keykeykeykeykeyk', 'iviviviviviviviviviviviviviviviviviviviviv'); --{serverError BAD_ARGUMENTS} IV is too long 3
SELECT decrypt('aes-128-cbc', 'text', 'keykeykeykeykeyk', 'iv'); --{serverError BAD_ARGUMENTS} IV is too short 2

--AAD
SELECT aes_decrypt_mysql('aes-128-ecb', 'text', 'key', 'IV', 1213); --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH} too many arguments

SELECT decrypt('aes-128-ecb', 'text', 'key', 'IV', 1213); --{serverError ILLEGAL_TYPE_OF_ARGUMENT} bad AAD type
SELECT decrypt('aes-128-gcm', 'text', 'key', 'IV', 1213); --{serverError ILLEGAL_TYPE_OF_ARGUMENT} bad AAD type

-- Invalid ciphertext should cause an error or produce garbage
SELECT ignore(decrypt('aes-128-ecb', 'hello there', '1111111111111111')); -- {serverError OPENSSL_ERROR} 1
SELECT ignore(decrypt('aes-128-cbc', 'hello there', '1111111111111111')); -- {serverError OPENSSL_ERROR} 2
SELECT ignore(decrypt('aes-128-ofb', 'hello there', '1111111111111111')); -- GIGO
SELECT ignore(decrypt('aes-128-ctr', 'hello there', '1111111111111111')); -- GIGO
SELECT decrypt('aes-128-ctr', '', '1111111111111111') == '';


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
SELECT 'aes-128-cbc' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;
SELECT 'aes-192-cbc' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;
SELECT 'aes-256-cbc' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;

SELECT 'aes-128-ecb' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;
SELECT 'aes-192-ecb' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;
SELECT 'aes-256-ecb' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;

SELECT 'aes-128-ofb' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;
SELECT 'aes-192-ofb' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;
SELECT 'aes-256-ofb' as mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key, iv), key, iv) == input FROM encryption_test;

SELECT 'Strict mode without key folding and proper key and iv lengths checks.';
SELECT 'aes-128-cbc' as mode, decrypt(mode, encrypt(mode, input, key16, iv), key16, iv) == input FROM encryption_test;
SELECT 'aes-192-cbc' as mode, decrypt(mode, encrypt(mode, input, key24, iv), key24, iv) == input FROM encryption_test;
SELECT 'aes-256-cbc' as mode, decrypt(mode, encrypt(mode, input, key32, iv), key32, iv) == input FROM encryption_test;

SELECT 'aes-128-ctr' as mode, decrypt(mode, encrypt(mode, input, key16, iv), key16, iv) == input FROM encryption_test;
SELECT 'aes-192-ctr' as mode, decrypt(mode, encrypt(mode, input, key24, iv), key24, iv) == input FROM encryption_test;
SELECT 'aes-256-ctr' as mode, decrypt(mode, encrypt(mode, input, key32, iv), key32, iv) == input FROM encryption_test;

SELECT 'aes-128-ecb' as mode, decrypt(mode, encrypt(mode, input, key16), key16) == input FROM encryption_test;
SELECT 'aes-192-ecb' as mode, decrypt(mode, encrypt(mode, input, key24), key24) == input FROM encryption_test;
SELECT 'aes-256-ecb' as mode, decrypt(mode, encrypt(mode, input, key32), key32) == input FROM encryption_test;

SELECT 'aes-128-ofb' as mode, decrypt(mode, encrypt(mode, input, key16, iv), key16, iv) == input FROM encryption_test;
SELECT 'aes-192-ofb' as mode, decrypt(mode, encrypt(mode, input, key24, iv), key24, iv) == input FROM encryption_test;
SELECT 'aes-256-ofb' as mode, decrypt(mode, encrypt(mode, input, key32, iv), key32, iv) == input FROM encryption_test;

SELECT 'GCM mode with IV';
SELECT 'aes-128-gcm' as mode, decrypt(mode, encrypt(mode, input, key16, iv), key16, iv) == input FROM encryption_test;
SELECT 'aes-192-gcm' as mode, decrypt(mode, encrypt(mode, input, key24, iv), key24, iv) == input FROM encryption_test;
SELECT 'aes-256-gcm' as mode, decrypt(mode, encrypt(mode, input, key32, iv), key32, iv) == input FROM encryption_test;

SELECT 'GCM mode with IV and AAD';
SELECT 'aes-128-gcm' as mode, decrypt(mode, encrypt(mode, input, key16, iv, 'AAD'), key16, iv, 'AAD') == input FROM encryption_test;
SELECT 'aes-192-gcm' as mode, decrypt(mode, encrypt(mode, input, key24, iv, 'AAD'), key24, iv, 'AAD') == input FROM encryption_test;
SELECT 'aes-256-gcm' as mode, decrypt(mode, encrypt(mode, input, key32, iv, 'AAD'), key32, iv, 'AAD') == input FROM encryption_test;


-- based on https://github.com/openssl/openssl/blob/master/demos/evp/aesgcm.c#L20
WITH
    unhex('eebc1f57487f51921c0465665f8ae6d1658bb26de6f8a069a3520293a572078f') as key,
    unhex('67ba0510262ae487d737ee6298f77e0c') as tag,
    unhex('99aa3e68ed8173a0eed06684') as iv,
    unhex('f56e87055bc32d0eeb31b2eacc2bf2a5') as plaintext,
    unhex('4d23c3cec334b49bdb370c437fec78de') as aad,
    unhex('f7264413a84c0e7cd536867eb9f21736') as ciphertext
SELECT
    hex(decrypt('aes-256-gcm', concat(ciphertext, tag), key, iv, aad)) as plaintext_actual,
    plaintext_actual = hex(plaintext);

-- tryDecrypt
CREATE TABLE decrypt_null (
  dt DateTime,
  user_id UInt32,
  encrypted String,
  iv String
) ENGINE = Memory;

INSERT INTO decrypt_null VALUES ('2022-08-02 00:00:00', 1, encrypt('aes-256-gcm', 'value1', 'keykeykeykeykeykeykeykeykeykey01', 'iv1'), 'iv1'), ('2022-09-02 00:00:00', 2, encrypt('aes-256-gcm', 'value2', 'keykeykeykeykeykeykeykeykeykey02', 'iv2'), 'iv2'), ('2022-09-02 00:00:01', 3, encrypt('aes-256-gcm', 'value3', 'keykeykeykeykeykeykeykeykeykey03', 'iv3'), 'iv3');

SELECT dt, user_id FROM decrypt_null WHERE (user_id > 0) AND (decrypt('aes-256-gcm', encrypted, 'keykeykeykeykeykeykeykeykeykey02', iv) = 'value2'); --{serverError OPENSSL_ERROR}
SELECT dt, user_id FROM decrypt_null WHERE (user_id > 0) AND (tryDecrypt('aes-256-gcm', encrypted, 'keykeykeykeykeykeykeykeykeykey02', iv) = 'value2');
SELECT dt, user_id, (tryDecrypt('aes-256-gcm', encrypted, 'keykeykeykeykeykeykeykeykeykey02', iv)) as value FROM decrypt_null ORDER BY user_id;

DROP TABLE encryption_test;
