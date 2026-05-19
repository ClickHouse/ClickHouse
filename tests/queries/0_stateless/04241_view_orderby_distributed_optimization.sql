-- Test for ORDER BY pushdown optimization into VIEWs over distributed tables.
-- A simple SELECT * style VIEW over a Distributed table should be transparent
-- for ORDER BY/LIMIT, so the underlying Distributed table sees the ORDER BY
-- and can use merge-sorted-streams instead of returning unsorted rows that the
-- coordinator would have to fully sort.

-- Tags: distributed

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS test_local_04241;
DROP TABLE IF EXISTS test_distributed_04241;
DROP VIEW IF EXISTS test_view_04241;

CREATE TABLE test_local_04241 (
    id UInt64,
    val String,
    ts DateTime
) ENGINE = MergeTree()
ORDER BY (id, ts);

CREATE TABLE test_distributed_04241 AS test_local_04241
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), test_local_04241, id);

INSERT INTO test_local_04241 SELECT number, toString(number), now() - number FROM numbers(100);

CREATE VIEW test_view_04241 AS
SELECT id, val, ts FROM test_distributed_04241;

-- Direct query against the Distributed table uses merge-sorted-streams (baseline).
SELECT 'Direct query has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_distributed_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- View query must also use merge-sorted-streams thanks to the pushdown.
SELECT 'View query has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

-- EXPLAIN PLAN: after pushdown the merge-sorted-streams step must appear in
-- the plan. Match only the merge step to keep the reference stable across
-- builds (the surrounding plan shape depends on `Distributed` internals).
SELECT trim(replaceRegexpAll(explain, '^\\s+', '')) AS step
FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10)
WHERE explain LIKE '%Merge sorted streams%';

-- Result correctness: pushdown must not change result rows.
SELECT 'View ORDER BY+LIMIT result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10
);

-- ORDER BY ... NULLS FIRST: modifier must survive pushdown.
SELECT 'NULLS FIRST has merge sort:',
    (SELECT count() > 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC NULLS FIRST LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS has_merge_sort;

SELECT 'NULLS FIRST result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC NULLS FIRST LIMIT 10
);

-- LIMIT ... WITH TIES: pushdown must be disabled (ties are computed globally
-- after ORDER BY, so per-shard truncation could drop tied rows).
SELECT 'WITH TIES disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10 WITH TIES)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'WITH TIES result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 10 WITH TIES
);

-- ORDER BY ... WITH FILL: pushdown must be disabled (WITH FILL synthesizes
-- rows; per-shard fills would produce a wrong final set after merging).
SELECT 'WITH FILL disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY id WITH FILL FROM 0 TO 5)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

-- Outer GROUP BY: pushdown must be disabled (it would otherwise break aggregate
-- projection matching and similar optimizations). The query must still produce
-- correct results.
SELECT 'GROUP BY result count:', count() FROM (
    SELECT id, count() FROM test_view_04241 GROUP BY id ORDER BY id LIMIT 5
);

-- Outer LIMIT ... OFFSET ...: pushdown must be disabled to preserve correctness.
SELECT 'OFFSET result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 5 OFFSET 3
);

-- Outer LIMIT BY: pushdown must be disabled because LIMIT BY is evaluated
-- globally on the coordinator after merging, so per-shard truncation could
-- drop candidates needed to fill each group.
SELECT 'LIMIT BY disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 1 BY id LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'LIMIT BY result count:', count() FROM (
    SELECT id FROM test_view_04241 ORDER BY ts DESC LIMIT 1 BY id LIMIT 10
);

-- Outer JOIN: pushdown must be disabled because the JOIN may filter or expand
-- rows after per-shard truncation, dropping rows that belong to the global top-N.
DROP TABLE IF EXISTS test_join_04241;
CREATE TABLE test_join_04241 (id UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_join_04241 SELECT number FROM numbers(50);

SELECT 'JOIN disables pushdown:',
    (SELECT count() = 0 FROM (EXPLAIN SELECT v.id FROM test_view_04241 v INNER JOIN test_join_04241 j ON v.id = j.id ORDER BY v.ts DESC LIMIT 10)
     WHERE explain LIKE '%Merge sorted streams%') AS no_merge_sort;

SELECT 'JOIN result count:', count() FROM (
    SELECT v.id FROM test_view_04241 v INNER JOIN test_join_04241 j ON v.id = j.id ORDER BY v.ts DESC LIMIT 10
);

DROP TABLE test_join_04241;
DROP VIEW test_view_04241;
DROP TABLE test_distributed_04241;
DROP TABLE test_local_04241;
