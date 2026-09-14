-- Both inputs are hash-scattered by the whole row, and a scatter does not push empty chunks, so a
-- partition whose share of the left input is empty gets nothing from the left scatter. The
-- partitioned transforms must not wait for a left chunk before draining their right input: doing so
-- deadlocks the pipeline once one partition holds its left port while another still needs right data.

-- The DISTINCT modes are executed as joins by default; this test covers the set-operation step.
SET optimize_rewrite_intersect_except_to_join = 0;

SELECT count() FROM
(
    SELECT number % 2 AS x FROM numbers_mt(10000000)
    EXCEPT ALL
    SELECT number AS x FROM numbers_mt(10000000)
)
SETTINGS max_threads = 8;

SELECT count() FROM
(
    SELECT number % 2 AS x FROM numbers_mt(10000000)
    INTERSECT ALL
    SELECT number AS x FROM numbers_mt(10000000)
)
SETTINGS max_threads = 8;

SELECT count() FROM
(
    SELECT number % 2 AS x FROM numbers_mt(10000000)
    EXCEPT DISTINCT
    SELECT number AS x FROM numbers_mt(10000000)
)
SETTINGS max_threads = 8;

SELECT count() FROM
(
    SELECT number AS x FROM numbers_mt(10000000)
    EXCEPT ALL
    SELECT number % 2 AS x FROM numbers_mt(10000000)
)
SETTINGS max_threads = 8;
