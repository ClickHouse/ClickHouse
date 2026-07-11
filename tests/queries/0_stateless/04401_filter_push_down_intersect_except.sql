-- Filter above INTERSECT/EXCEPT (ALL and DISTINCT) must be pushed into every input
-- branch, exactly as for UNION ALL, so each input prunes with its own index.
-- https://github.com/ClickHouse/ClickHouse/issues/110113

-- Parallel replicas replace ReadFromMergeTree with a remote read step, changing the
-- plan shape the EXPLAIN assertions below inspect; pin it off for a stable local plan.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_intex_l;
DROP TABLE IF EXISTS t_intex_r;

CREATE TABLE t_intex_l (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_intex_r (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_intex_l SELECT number FROM numbers(1000);
INSERT INTO t_intex_r SELECT number FROM numbers(1000);

-- Each set operator: the pushed key condition (a in [5, 5]) must appear on BOTH
-- ReadFromMergeTree branches (count() = 2), proving the filter reached each input.

SELECT 'INTERSECT ALL', count() FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_l INTERSECT ALL SELECT a FROM t_intex_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';

SELECT 'INTERSECT DISTINCT', count() FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_l INTERSECT DISTINCT SELECT a FROM t_intex_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';

SELECT 'EXCEPT ALL', count() FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';

SELECT 'EXCEPT DISTINCT', count() FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_l EXCEPT DISTINCT SELECT a FROM t_intex_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';

-- Correctness: multiplicity/semantics preserved with the filter pushed down.
DROP TABLE t_intex_l;
DROP TABLE t_intex_r;
CREATE TABLE t_intex_l (a UInt64) ENGINE = Memory;
CREATE TABLE t_intex_r (a UInt64) ENGINE = Memory;
INSERT INTO t_intex_l VALUES (5),(5),(5),(7);
INSERT INTO t_intex_r VALUES (5),(5),(9);

SELECT 'res INTERSECT ALL', a FROM (SELECT a FROM t_intex_l INTERSECT ALL SELECT a FROM t_intex_r) WHERE a = 5 ORDER BY a;
SELECT 'res INTERSECT DISTINCT', a FROM (SELECT a FROM t_intex_l INTERSECT DISTINCT SELECT a FROM t_intex_r) WHERE a = 5 ORDER BY a;
SELECT 'res EXCEPT ALL', a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE a = 5 ORDER BY a;
SELECT 'res EXCEPT DISTINCT', a FROM (SELECT a FROM t_intex_l EXCEPT DISTINCT SELECT a FROM t_intex_r) WHERE a = 5 ORDER BY a;

DROP TABLE t_intex_l;
DROP TABLE t_intex_r;

-- A filter over a Variant/Dynamic column can throw at runtime on the concrete alternative
-- a row carries (e.g. ilike over the Tuple alternative). INTERSECT/EXCEPT eliminate rows, so
-- pushing such a filter into the branches would evaluate it on rows the set op removes and
-- surface an error the unoptimized plan never produces. The pushdown must be skipped for
-- these columns. https://github.com/ClickHouse/ClickHouse/issues/110113
SELECT 'variant except', count() FROM (SELECT c0 FROM ((SELECT 'a') EXCEPT ALL SELECT (1, 2))(c0)) AS t0 WHERE t0.c0 ILIKE t0.c0 = true;
SELECT 'variant intersect', count() FROM (SELECT c0 FROM ((SELECT 'a') INTERSECT ALL SELECT (1, 2))(c0)) AS t0 WHERE t0.c0 ILIKE t0.c0 = true;
