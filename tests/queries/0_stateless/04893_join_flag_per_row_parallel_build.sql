-- Multi-disjunct FULL JOIN with a parallel build: unmatched-right counts must be exact
-- (a lost or duplicated per-row flag silently drops or double-counts rows).
-- Right: 200000 rows. [0, 100) match via the first disjunct, [100000, 100100) via the second.
-- matched pairs = 200, unmatched right = 199800, total = 200000.

SET max_threads = 8;
SET join_algorithm = 'parallel_hash';
SET parallel_hash_join_threshold = 1;
SET max_block_size = 8192;

DROP TABLE IF EXISTS t1_04893;
DROP TABLE IF EXISTS t2_04893;

CREATE TABLE t1_04893 (number UInt64) ENGINE = MergeTree ORDER BY number;
CREATE TABLE t2_04893 (number UInt64) ENGINE = MergeTree ORDER BY number;

INSERT INTO t1_04893 SELECT number FROM numbers(100);
INSERT INTO t2_04893 SELECT number FROM numbers(200000);

SELECT count(), sum(t2_04893.number), sum(t1_04893.number)
FROM t1_04893
FULL JOIN t2_04893
    ON t1_04893.number = t2_04893.number OR t1_04893.number + 100000 = t2_04893.number;

DROP TABLE t1_04893;
DROP TABLE t2_04893;
