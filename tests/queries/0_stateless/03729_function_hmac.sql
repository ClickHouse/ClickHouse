-- Tags: no-fasttest, no-openssl-fips
-- The function HMAC requires openssl library disabled in fasttest builds.

-- HMAC basic function, test it's case insensitivity and correctness with known values
SELECT
    hex(hmac('md5', 'The quick brown fox jumps over the lazy dog', 'secret_key'))    AS hmac_md5,
    hex(Hmac('sha1', 'The quick brown fox jumps over the lazy dog', 'secret_key'))   AS hmac_sha1,
    hex(HMac('sha224', 'The quick brown fox jumps over the lazy dog', 'secret_key')) AS hmac_sha224,
    hex(HMAc('sha256', 'The quick brown fox jumps over the lazy dog', 'secret_key')) AS hmac_sha256,
    hex(hmAc('sha384', 'The quick brown fox jumps over the lazy dog', 'secret_key')) AS hmac_sha384,
    hex(HMAC('sha512', 'The quick brown fox jumps over the lazy dog', 'secret_key')) AS hmac_sha512
FORMAT Vertical;

SELECT '';

-- Test output lengths for all supported hash algorithms
SELECT length(HMAC('md4', 'test', 'key')); -- MD4 produces 16 bytes
SELECT length(HMAC('md5', 'test', 'key')); -- MD5 produces 16 bytes
SELECT length(HMAC('mdc2', 'test', 'key')); -- MDC2 produces 16 bytes
SELECT length(HMAC('ripemd', 'test', 'key')); -- ripemd produces 20 bytes
SELECT length(HMAC('sha1', 'test', 'key')); -- SHA1 produces 20 bytes
SELECT length(HMAC('sha224', 'test', 'key')); -- SHA224 produces 28 bytes
SELECT length(HMAC('sha256', 'test', 'key')); -- SHA256 produces 32 bytes
SELECT length(HMAC('sha384', 'test', 'key')); -- SHA384 produces 48 bytes
SELECT length(HMAC('sha512', 'test', 'key')); -- SHA512 produces 64 bytes
SELECT length(HMAC('sha512-224', 'test', 'key')); -- SHA512/224 produces 28 bytes
SELECT length(HMAC('sha512-256', 'test', 'key')); -- SHA512/256 produces 32 bytes
SELECT length(HMAC('sha3-224', 'test', 'key')); -- SHA3-224 produces 28 bytes
SELECT length(HMAC('sha3-256', 'test', 'key')); -- SHA3-256 produces 32 bytes
SELECT length(HMAC('sha3-384', 'test', 'key')); -- SHA3-384 produces 48 bytes
SELECT length(HMAC('sha3-512', 'test', 'key')); -- SHA3-512 produces 64 bytes
SELECT length(HMAC('blake2b512', 'test', 'key')); -- BLAKE2b-512 produces 64 bytes
SELECT length(HMAC('blake2s256', 'test', 'key')); -- BLAKE2s-256 produces 32 bytes
SELECT length(HMAC('sm3', 'test', 'key')); -- SM3 produces 32 bytes
SELECT length(HMAC('whirlpool', 'test', 'key')); -- Whirlpool produces 64 bytes

SELECT '';

-- Test with empty strings
SELECT length(HMAC('sha256', '', 'key')) = 32;
SELECT length(HMAC('sha256', 'message', '')) = 32;
SELECT length(HMAC('sha256', '', '')) = 32;

SELECT '';

-- Test with table data
CREATE TEMPORARY TABLE hmac_test (message String, key String);
INSERT INTO hmac_test VALUES ('hello', 'world'), ('foo', 'bar'), ('test', 'key');
SELECT message, key, hex(HMAC('sha256', message, key)) AS hmac_hex FROM hmac_test ORDER BY message;
DROP TABLE hmac_test;

-- Test invalid algorithm (should throw error)
SELECT HMAC('invalid_algo', 'message', 'key'); -- { serverError BAD_ARGUMENTS }

-- Test big column
SELECT hmac('sha256', toString(number), 'key') FROM system.numbers LIMIT 100000 FORMAT Null;
