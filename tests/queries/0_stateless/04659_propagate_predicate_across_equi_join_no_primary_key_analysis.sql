SET enable_analyzer = 1;
-- Under parallel replicas the reads are remote and the pass bails
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS prop_nopk_src;
DROP TABLE IF EXISTS prop_nopk_dst;

CREATE TABLE prop_nopk_src (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prop_nopk_dst (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;

INSERT INTO prop_nopk_src SELECT number, toString(number) FROM numbers(100000);
INSERT INTO prop_nopk_dst SELECT number, toString(number) FROM numbers(100000);

-- With PK analysis on: 1 occurrence = source side only, >= 2 = copied
SELECT 'primary key analysis on',
       countIf(explain LIKE '%ilter column:%k = 12345%') >= 2
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_src WHERE k = 12345) AS s
    INNER JOIN prop_nopk_dst AS d ON s.k = d.k
);

-- Without `use_primary_key` the copy would be a full scan filter, so the pass stays off
SELECT 'primary key analysis off',
       countIf(explain LIKE '%ilter column:%k = 12345%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM (SELECT * FROM prop_nopk_src WHERE k = 12345) AS s
    INNER JOIN prop_nopk_dst AS d ON s.k = d.k
    SETTINGS use_primary_key = 0
);

SELECT 'correctness',
       (SELECT count() FROM (SELECT * FROM prop_nopk_src WHERE k BETWEEN 100 AND 200) AS s
        INNER JOIN prop_nopk_dst AS d ON s.k = d.k
        SETTINGS use_primary_key = 0)
     - (SELECT count() FROM (SELECT * FROM prop_nopk_src WHERE k BETWEEN 100 AND 200) AS s
        INNER JOIN prop_nopk_dst AS d ON s.k = d.k);

DROP TABLE prop_nopk_src;
DROP TABLE prop_nopk_dst;
