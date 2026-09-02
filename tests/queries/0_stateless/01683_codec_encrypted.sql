-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

DROP TABLE IF EXISTS encryption_test;
CREATE TABLE encryption_test (i Int, s String Codec(AES_128_GCM_SIV)) ENGINE = MergeTree ORDER BY i;

INSERT INTO encryption_test VALUES (1, 'Some plaintext');
SELECT * FROM encryption_test;

DROP TABLE encryption_test;

CREATE TABLE encryption_test (i Int, s String Codec(AES_256_GCM_SIV)) ENGINE = MergeTree ORDER BY i;

INSERT INTO encryption_test VALUES (1, 'Some plaintext');
SELECT * FROM encryption_test;

DROP TABLE encryption_test;
