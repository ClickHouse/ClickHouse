-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

DROP TABLE IF EXISTS test_01344;

CREATE TABLE test_01344 (x String, INDEX idx (x) TYPE set(10) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO test_01344 VALUES ('Hello, world');
SELECT NULL FROM test_01344 WHERE ignore(1) = NULL;
SELECT NULL FROM test_01344 WHERE encrypt(ignore(encrypt(NULL, '0.0001048577', lcm(2, 65537), NULL, inf, NULL), lcm(-2, 1048575)), '-0.0000000001', lcm(NULL, NULL)) = NULL;
SELECT NULL FROM test_01344 WHERE ignore(x, lcm(NULL, 1048576), -2) = NULL;

DROP TABLE test_01344;
