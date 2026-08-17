DROP TABLE IF EXISTS t_04743;
CREATE TABLE t_04743 (A Array(UInt32), B Array(UInt32), n UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04743 VALUES ([1,2,3],[1,2],1), ([4,5],[],2), ([6],[7,8,9],3);

-- arrayJoin in the aggregate argument multiplies rows, so the stored row count (3) is not the answer
SELECT count(arrayJoin(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(unnest(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(arrayJoin(A) + 1) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(arrayJoin(B)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(arrayJoin(arrayJoin([A, B]))) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;

-- the same values with the optimization off
SELECT count(arrayJoin(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;
SELECT count(unnest(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;
SELECT count(arrayJoin(A) + 1) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;
SELECT count(arrayJoin(B)) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;
SELECT count(arrayJoin(arrayJoin([A, B]))) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;

-- aggregates without arrayJoin keep the optimization
SELECT count() FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(*) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(1) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(n) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count() FROM t_04743 ARRAY JOIN A SETTINGS optimize_trivial_count_query = 1;

-- plans: the optimization is refused for the arrayJoin argument and kept otherwise
SELECT count() > 0 FROM (EXPLAIN SELECT count(arrayJoin(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1)
WHERE explain ILIKE '%Optimized trivial count%';
SELECT count() > 0 FROM (EXPLAIN SELECT count(arrayJoin(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1)
WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_04743 SETTINGS optimize_trivial_count_query = 1)
WHERE explain ILIKE '%Optimized trivial count%';
SELECT count() > 0 FROM (EXPLAIN SELECT count(n) FROM t_04743 SETTINGS optimize_trivial_count_query = 1)
WHERE explain ILIKE '%Optimized trivial count%';

DROP TABLE t_04743;
