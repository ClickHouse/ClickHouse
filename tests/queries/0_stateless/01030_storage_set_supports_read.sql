DROP TABLE IF EXISTS userid_test;

SET use_index_for_in_with_subqueries = 1;

CREATE TABLE userid_test (userid UInt64) ENGINE = MergeTree() PARTITION BY (intDiv(userid, 500)) ORDER BY (userid) SETTINGS index_granularity = 8192;

INSERT INTO userid_test VALUES (1),(2),(3),(4),(5);

DROP TABLE IF EXISTS userid_set;

CREATE TABLE userid_set(userid UInt64) ENGINE = Set;

INSERT INTO userid_set VALUES (1),(2),(3);

SELECT * FROM userid_test WHERE userid IN (1, 2, 3);

SELECT * FROM userid_test WHERE toUInt64(1) IN (userid_set);

SELECT * FROM userid_test WHERE userid IN (userid_set);

DROP TABLE userid_test;
DROP TABLE userid_set;
