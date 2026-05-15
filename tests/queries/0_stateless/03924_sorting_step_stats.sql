CREATE TABLE t (c UInt64) ENGINE=MergeTree;

INSERT INTO t SELECT * FROM numbers(999);

SET enable_analyzer = 1,
    query_plan_join_swap_table = 0,
    enable_parallel_replicas = 0;


SELECT '-------------- Limit < table size -------------';
SELECT explain FROM
(
    EXPLAIN keep_logical_steps=1, actions=1
    SELECT count() FROM
        (SELECT * FROM t) AS t1,
        (SELECT * FROM t ORDER BY c LIMIT 22) AS t2
    WHERE t1.c = t2.c
)
WHERE (explain LIKE '%Join%') OR (explain LIKE '%Sorting%') OR (explain LIKE '%Limit%') OR (explain LIKE '%ReadFromMergeTree%');


SELECT '-------------- Limit > table size -------------';
SELECT explain FROM
(
    EXPLAIN keep_logical_steps=1, actions=1
    SELECT count() FROM
        (SELECT * FROM t) AS t1,
        (SELECT * FROM t ORDER BY c LIMIT 5000) AS t2
    WHERE t1.c = t2.c
)
WHERE (explain LIKE '%Join%') OR (explain LIKE '%Sorting%') OR (explain LIKE '%Limit%') OR (explain LIKE '%ReadFromMergeTree%');
