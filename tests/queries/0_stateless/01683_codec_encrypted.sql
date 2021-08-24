DROP TABLE IF EXISTS encryption_test;
CREATE TABLE encryption_test (i Int, s String Codec(Encrypted('AES-128-GCM-SIV'))) ENGINE = MergeTree ORDER BY i;

INSERT INTO encryption_test VALUES (1, 'Some plaintext');
SELECT * FROM encryption_test;

DROP TABLE encryption_test;
