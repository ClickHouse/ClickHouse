-- Tags: no-fasttest

SELECT base32Encode(randomString(1, 100)) FROM numbers(1000) FORMAT Null;
SELECT base58Encode(randomString(1, 100)) FROM numbers(1000) FORMAT Null;