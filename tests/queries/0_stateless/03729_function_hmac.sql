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

-- Test different hash algorithms produce different lengths
SELECT length(HMAC('md5', 'test', 'key')) = 16; -- MD5 produces 16 bytes
SELECT length(HMAC('sha1', 'test', 'key')) = 20; -- SHA1 produces 20 bytes
SELECT length(HMAC('sha224', 'test', 'key')) = 28; -- SHA224 produces 28 bytes
SELECT length(HMAC('sha256', 'test', 'key')) = 32; -- SHA256 produces 32 bytes
SELECT length(HMAC('sha384', 'test', 'key')) = 48; -- SHA384 produces 48 bytes
SELECT length(HMAC('sha512', 'test', 'key')) = 64; -- SHA512 produces 64 bytes

SELECT '';

-- Test with empty strings
SELECT length(HMAC('sha256', '', 'key')) = 32;
SELECT length(HMAC('sha256', 'message', '')) = 32;
SELECT length(HMAC('sha256', '', '')) = 32;

SELECT '';

-- Test with table data
CREATE TEMPORARY TABLE hmac_test (message String, key String);
INSERT INTO hmac_test VALUES ('hello', 'world'), ('foo', 'bar'), ('test', 'key');
SELECT message, key, hex(HMAC('sha256', message, key)) AS hmac_length FROM hmac_test ORDER BY message;
DROP TABLE hmac_test;

-- Test invalid algorithm (should throw error)
SELECT HMAC('invalid_algo', 'message', 'key'); -- { serverError BAD_ARGUMENTS }
