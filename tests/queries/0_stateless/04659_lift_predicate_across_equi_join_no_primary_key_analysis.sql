SET enable_analyzer = 1;
-- The lift targets local MergeTree reads; under parallel replicas the plan reads through
-- remote-replica steps and the pass correctly bails, changing the EXPLAIN output
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS lift_nopk_src;
DROP TABLE IF EXISTS lift_nopk_dst;

CREATE TABLE lift_nopk_src (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE lift_nopk_dst (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;

INSERT INTO lift_nopk_src SELECT number, toString(number) FROM numbers(100000);
INSERT INTO lift_nopk_dst SELECT number, toString(number) FROM numbers(100000);

-- Sanity check: with primary key analysis enabled the predicate is lifted to the target side
-- (1 occurrence = source side only, >= 2 = lifted to target too)
SELECT 'primary key analysis on',
       countIf(explain LIKE '%ilter column:%k = 12345%') >= 2
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_nopk_src WHERE k = 12345) AS s
    INNER JOIN lift_nopk_dst AS d ON s.k = d.k
);

-- With `use_primary_key = 0` the copied conjunct could never drive index pruning
-- and would survive as a plain target-side filter over the full scan, so the pass must stay off
SELECT 'primary key analysis off',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM lift_nopk_src WHERE k = 12345) AS s
    INNER JOIN lift_nopk_dst AS d ON s.k = d.k
    SETTINGS use_primary_key = 0
);

SELECT 'correctness',
       (SELECT count() FROM (SELECT * FROM lift_nopk_src WHERE k BETWEEN 100 AND 200) AS s
        INNER JOIN lift_nopk_dst AS d ON s.k = d.k
        SETTINGS use_primary_key = 0)
     - (SELECT count() FROM (SELECT * FROM lift_nopk_src WHERE k BETWEEN 100 AND 200) AS s
        INNER JOIN lift_nopk_dst AS d ON s.k = d.k);

DROP TABLE lift_nopk_src;
DROP TABLE lift_nopk_dst;
