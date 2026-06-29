-- Tags: no-fasttest

SELECT base32Decode(s) FROM (SELECT base32Encode(randomString(100)) AS s FROM numbers(100000)) FORMAT Null;
SELECT base58Decode(s) FROM (SELECT base58Encode(randomString(100)) AS s FROM numbers(100000)) FORMAT Null;
SELECT base64Decode(s) FROM (SELECT base64Encode(randomString(100)) AS s FROM numbers(100000)) FORMAT Null;