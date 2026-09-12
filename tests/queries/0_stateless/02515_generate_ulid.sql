-- Tags: no-fasttest

SELECT generateULID(1) != generateULID(2), toTypeName(generateULID());

-- The value is unpredictable, so assert the number of distinct values and never the values. The
-- dictionary holds two entries and the query four rows, so executing once per dictionary entry
-- caps the count at two. A threshold, not an equality: the function promises nothing about uniqueness.
DROP TABLE IF EXISTS t_ulid_lc;
CREATE TABLE t_ulid_lc (id UInt32, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ulid_lc SELECT number, ['a', 'a', 'bb', 'bb'][number + 1] FROM numbers(4);

SELECT uniqExact(v) > 2 FROM (SELECT generateULID(s) AS v FROM t_ulid_lc);

DROP TABLE t_ulid_lc;
