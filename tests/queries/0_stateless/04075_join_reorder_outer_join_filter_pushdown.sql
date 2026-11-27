-- Regression test: join reorder must not push INNER JOIN filter conditions
-- into an outer join's ON clause, where they only affect matching
-- but do not filter preserved-side rows.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (id UInt64, value String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (0, 'a'), (1, 'b'), (2, 'c');
INSERT INTO t2 VALUES (0, 'x'), (1, 'y'), (3, 'z');
INSERT INTO t3 VALUES (0, 'p'), (1, 'q'), (4, 'r');

-- The condition `t2.value = 'x'` is part of the INNER JOIN.
-- It must NOT be pushed into the RIGHT JOIN's ON clause by join reorder,
-- because that changes the semantics (preserved-side conditions in an
-- outer join ON clause only affect matching, not filtering).

SELECT count()
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id AND t1.value = 'a'
INNER JOIN t3 ON t2.id = t3.id AND t2.value = 'x';

SELECT count()
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id AND t1.value = 'a'
INNER JOIN t3 ON t2.id = t3.id AND t2.value = 'x'
SETTINGS query_plan_split_filter = 0;

-- Same for LEFT JOIN (condition on preserved left side from parent INNER JOIN)
SELECT count()
FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.value = 'x'
INNER JOIN t3 ON t1.id = t3.id AND t1.value = 'a';

SELECT count()
FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.value = 'x'
INNER JOIN t3 ON t1.id = t3.id AND t1.value = 'a'
SETTINGS query_plan_split_filter = 0;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
