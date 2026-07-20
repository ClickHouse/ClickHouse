-- Tags: no-old-analyzer

-- INNER band join on randomized duplicate-heavy data, for all four strict/loose bracket
-- combinations: verified against a brute-force cross-join oracle (comma-join syntax with the
-- conditions in WHERE; `cross_to_inner_join_rewrite` is disabled so that it cannot be routed
-- through the band join) and byte-for-byte against `ie_join` on the same queries.

-- Keep the written join order so the checks below exercise the orientation as written
-- instead of whatever the join order optimizer prefers.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS bo_p;
DROP TABLE IF EXISTS bo_i;

CREATE TABLE bo_p (id UInt32, t Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bo_i (id UInt32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;

-- Small domain for heavy key duplication; interval widths from -2 (empty) to 4.
INSERT INTO bo_p SELECT number, ((number * number + 6789) % 2147483647) % 40 FROM numbers(400);
INSERT INTO bo_i
    SELECT number, x, x + (number % 7) - 2
    FROM (SELECT number, (((number + 100) * (number + 100) + 12345) % 2147483647) % 40 AS x FROM numbers(400));

SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';

SELECT '>= <=' AS brackets,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p, bo_i i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT '> <=' AS brackets,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p, bo_i i WHERE p.t > i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t > i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM bo_p p JOIN bo_i i ON p.t > i.lo AND p.t <= i.hi) AS cnt;

SELECT '>= <' AS brackets,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p, bo_i i WHERE p.t >= i.lo AND p.t < i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t < i.hi) AS cnt;

SELECT '> <' AS brackets,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p, bo_i i WHERE p.t > i.lo AND p.t < i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t > i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM bo_p p JOIN bo_i i ON p.t > i.lo AND p.t < i.hi) AS cnt;

-- All the non-key columns of both sides travel through the join correctly, not only the ids
SELECT 'full rows',
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM bo_p p, bo_i i WHERE p.t >= i.lo AND p.t <= i.hi);

-- `BETWEEN` desugars to the loose band
SELECT 'between',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t BETWEEN i.lo AND i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bo_p p JOIN bo_i i ON p.t >= i.lo AND p.t <= i.hi);

DROP TABLE bo_p;
DROP TABLE bo_i;
