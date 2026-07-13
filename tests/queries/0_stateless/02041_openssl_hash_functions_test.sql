-- Tags: no-fasttest, no-openssl-fips

SELECT hex(halfMD5('test'));
SELECT hex(MD4('test'));
SELECT hex(MD5('test'));
SELECT hex(SHA1('test'));
SELECT hex(SHA224('test'));
SELECT hex(SHA256('test'));
SELECT hex(SHA384('test'));
SELECT hex(SHA512('test'));
SELECT hex(SHA512_256('test'));
