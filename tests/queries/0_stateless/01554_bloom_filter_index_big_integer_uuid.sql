SET allow_experimental_bigint_types = 1;

CREATE TABLE 01154_test (x Int128, INDEX ix_x x TYPE bloom_filter(0.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY x SETTINGS index_granularity=8192;
INSERT INTO 01154_test VALUES (1), (2), (3);
SELECT x FROM 01154_test WHERE x = 1;
SELECT x FROM 01154_test WHERE x IN (1, 2);
DROP TABLE 01154_test;

CREATE TABLE 01154_test (x Int256, INDEX ix_x x TYPE bloom_filter(0.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY x SETTINGS index_granularity=8192;
INSERT INTO 01154_test VALUES (1), (2), (3);
SELECT x FROM 01154_test WHERE x = 1;
SELECT x FROM 01154_test WHERE x IN (1, 2);
DROP TABLE 01154_test;

CREATE TABLE 01154_test (x UInt256, INDEX ix_x x TYPE bloom_filter(0.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY x SETTINGS index_granularity=8192;
INSERT INTO 01154_test VALUES (1), (2), (3);
SELECT x FROM 01154_test WHERE x = 1;
SELECT x FROM 01154_test WHERE x IN (1, 2);
DROP TABLE 01154_test;

CREATE TABLE 01154_test (x UUID, INDEX ix_x x TYPE bloom_filter(0.01) GRANULARITY 1) ENGINE = MergeTree() ORDER BY x SETTINGS index_granularity=8192;
INSERT INTO 01154_test VALUES (toUUID(1)), (toUUID(2)), (toUUID(3));
SELECT x FROM 01154_test WHERE x = toUUID(1);
SELECT x FROM 01154_test WHERE x IN (toUUID(1), toUUID(2));
DROP TABLE 01154_test;
