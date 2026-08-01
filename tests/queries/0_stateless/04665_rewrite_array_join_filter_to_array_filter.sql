SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_aj_filter;
CREATE TABLE t_aj_filter
(
    id Int32,
    A Array(String),
    B Array(String),
    C Array(String)
)
ENGINE = Memory;

INSERT INTO t_aj_filter VALUES
    (1, ['X-A', 'P'], ['X-B', 'Q'], ['X-C', 'R']),
    (2, ['P'], ['X-B'], ['X-C']);

-- Main issue example: multiple independent arrayJoin + AND predicates.
SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c
FROM t_aj_filter
WHERE a = 'X-A' AND b = 'X-B' AND c = 'X-C'
ORDER BY a, b, c;

-- Rewrite should introduce arrayFilter and drop the pushed WHERE conjuncts.
SET optimize_rewrite_array_join_filter_to_array_filter = 1;
EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c
FROM t_aj_filter
WHERE a = 'X-A' AND b = 'X-B' AND c = 'X-C';

-- Explicit: main rewrite drops WHERE entirely (no WHERE section in query tree).
SELECT countIf(explain = '  WHERE') AS where_sections
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT arrayJoin(A) AS a, arrayJoin(B) AS b, arrayJoin(C) AS c
    FROM t_aj_filter
    WHERE a = 'X-A' AND b = 'X-B' AND c = 'X-C'
);

-- Setting off: no arrayFilter rewrite.
SET optimize_rewrite_array_join_filter_to_array_filter = 0;
EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(A) AS a FROM t_aj_filter WHERE a = 'X-A';

SET optimize_rewrite_array_join_filter_to_array_filter = 1;

-- Captured row-constant column in the predicate.
SELECT arrayJoin(A) AS a, id
FROM t_aj_filter
WHERE a = concat('X-', 'A') AND id = 1
ORDER BY a, id;

EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(A) AS a FROM t_aj_filter WHERE a = toString(id);

-- Same column, multiple conjuncts: merge into one arrayFilter lambda (and inside lambda).
EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(A) AS a
FROM t_aj_filter
WHERE a = 'X-A' AND a != 'P';

SELECT
    countIf(explain LIKE '%function_name: arrayFilter%') AS array_filter_nodes,
    countIf(explain = '  WHERE') AS where_sections,
    countIf(explain LIKE '%function_name: and%') AS and_nodes
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT arrayJoin(A) AS a
    FROM t_aj_filter
    WHERE a = 'X-A' AND a != 'P'
);

-- WITH alias shares one arrayJoin node between projection and WHERE.
SELECT a
FROM
(
    WITH arrayJoin(A) AS a
    SELECT a FROM t_aj_filter WHERE a = 'X-A'
)
ORDER BY a;

EXPLAIN QUERY TREE run_passes = 1
WITH arrayJoin(A) AS a
SELECT a FROM t_aj_filter WHERE a = 'X-A';

SELECT
    countIf(explain LIKE '%function_name: arrayFilter%') AS array_filter_nodes,
    countIf(explain = '  WHERE') AS where_sections
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    WITH arrayJoin(A) AS a
    SELECT a FROM t_aj_filter WHERE a = 'X-A'
);

-- ARRAY JOIN form (single expression).
SELECT a
FROM t_aj_filter
ARRAY JOIN A AS a
WHERE a = 'X-A'
ORDER BY a;

EXPLAIN QUERY TREE run_passes = 1
SELECT a FROM t_aj_filter ARRAY JOIN A AS a WHERE a = 'X-A';

-- WHERE-only arrayJoin must keep expansion (cardinality): two matching elements -> two rows.
SELECT count()
FROM
(
    SELECT arrayJoin(['X', 'X', 'Y']) AS a WHERE a = 'X'
);

EXPLAIN QUERY TREE run_passes = 1
SELECT 1 FROM system.one WHERE arrayJoin(['X', 'Y']) = 'X';

-- Negative: predicate relating two arrayJoin columns — do not rewrite.
EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(A) AS a, arrayJoin(B) AS b FROM t_aj_filter WHERE a = b;

-- Negative: OR across different arrayJoin columns — do not rewrite.
EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(A) AS a, arrayJoin(B) AS b FROM t_aj_filter WHERE a = 'X-A' OR b = 'X-B';

-- Negative: non-deterministic predicate — do not rewrite.
EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(A) AS a FROM t_aj_filter WHERE a = 'X-A' AND rand() >= 0;

-- Negative: conjunct with subquery must not be pushed into arrayFilter.
EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(A) AS a FROM t_aj_filter WHERE a IN (SELECT 'X-A');

SELECT
    countIf(explain LIKE '%function_name: arrayFilter%') AS array_filter_nodes,
    countIf(explain = '  WHERE') AS where_sections
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT arrayJoin(A) AS a FROM t_aj_filter WHERE a IN (SELECT 'X-A')
);

-- Negative: LEFT ARRAY JOIN — do not rewrite.
EXPLAIN QUERY TREE run_passes = 1
SELECT a FROM t_aj_filter LEFT ARRAY JOIN A AS a WHERE a = 'X-A';

-- Negative: multi-array ARRAY JOIN — do not rewrite.
EXPLAIN QUERY TREE run_passes = 1
SELECT a, b FROM t_aj_filter ARRAY JOIN A AS a, B AS b WHERE a = 'X-A';

-- User-written arrayFilter must still accept an additional pushed filter.
EXPLAIN QUERY TREE run_passes = 1
SELECT arrayJoin(arrayFilter(x -> x != '', A)) AS a
FROM t_aj_filter
WHERE a = 'X-A';

DROP TABLE t_aj_filter;
