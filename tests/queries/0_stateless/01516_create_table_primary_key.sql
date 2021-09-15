-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01516;
CREATE DATABASE test_01516 ENGINE=Ordinary;     -- Full ATTACH requires UUID with Atomic
USE test_01516;

DROP TABLE IF EXISTS primary_key_test;

CREATE TABLE primary_key_test(v Int32, PRIMARY KEY(v)) ENGINE=ReplacingMergeTree ORDER BY v;
INSERT INTO primary_key_test VALUES (1), (1), (1);
DETACH TABLE primary_key_test;
ATTACH TABLE primary_key_test(v Int32, PRIMARY KEY(v)) ENGINE=ReplacingMergeTree ORDER BY v;
SELECT * FROM primary_key_test FINAL;
DROP TABLE primary_key_test;

CREATE TABLE primary_key_test(v Int32) ENGINE=ReplacingMergeTree ORDER BY v PRIMARY KEY(v);
INSERT INTO primary_key_test VALUES (1), (1), (1);
DETACH TABLE primary_key_test;
ATTACH TABLE primary_key_test(v Int32) ENGINE=ReplacingMergeTree ORDER BY v PRIMARY KEY(v);
SELECT * FROM primary_key_test FINAL;
DROP TABLE primary_key_test;

CREATE TABLE primary_key_test(v1 Int32, v2 Int32, PRIMARY KEY(v1, v2)) ENGINE=ReplacingMergeTree ORDER BY (v1, v2);
INSERT INTO primary_key_test VALUES (1, 1), (1, 1), (1, 1);
DETACH TABLE primary_key_test;
ATTACH TABLE primary_key_test(v1 Int32, v2 Int32, PRIMARY KEY(v1, v2)) ENGINE=ReplacingMergeTree ORDER BY (v1, v2);
SELECT * FROM primary_key_test FINAL;
DROP TABLE primary_key_test;

CREATE TABLE primary_key_test(v1 Int32, v2 Int32) ENGINE=ReplacingMergeTree ORDER BY (v1, v2) PRIMARY KEY(v1, v2);
INSERT INTO primary_key_test VALUES (1, 1), (1, 1), (1, 1);
DETACH TABLE primary_key_test;
ATTACH TABLE primary_key_test(v1 Int32, v2 Int32) ENGINE=ReplacingMergeTree ORDER BY (v1, v2) PRIMARY KEY(v1, v2);
SELECT * FROM primary_key_test FINAL;
DROP TABLE primary_key_test;

CREATE TABLE primary_key_test(v1 Int64, v2 Int32, v3 String, PRIMARY KEY(v1, gcd(v1, v2))) ENGINE=ReplacingMergeTree ORDER BY v1; -- { serverError 36; }

CREATE TABLE primary_key_test(v1 Int64, v2 Int32, v3 String, PRIMARY KEY(v1, gcd(v1, v2))) ENGINE=ReplacingMergeTree ORDER BY (v1, gcd(v1, v2));

INSERT INTO primary_key_test VALUES(7, 14, 'hello'), (2, 2, 'world'), (7, 14, 'duplicate');

SELECT v1, v2 FROM primary_key_test FINAL ORDER BY v1, v2;

DROP TABLE primary_key_test;

DROP DATABASE test_01516;
