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

-- A deterministic predicate can still throw on some values: intDiv(1, c0) throws on a c0 = 0 row.
-- INTERSECT/EXCEPT remove that row before the top filter runs, so without the optimization the
-- query returns 1. Pushing the filter into the branches would evaluate intDiv on the eliminated
-- 0 row and throw ILLEGAL_DIVISION. The pushdown must be skipped for throwing predicates.
SELECT 'intdiv except', count() FROM (SELECT c0 FROM ((SELECT 1) EXCEPT ALL SELECT 0)(c0)) WHERE intDiv(1, c0) = 1;
SELECT 'intdiv intersect', count() FROM (SELECT c0 FROM ((SELECT 1) INTERSECT ALL SELECT 0)(c0)) WHERE intDiv(1, c0) = 1;

-- INTERSECT/EXCEPT compare whole rows: the entire branch header is the set key. When the parent
-- needs no branch column (count()) the pushed filter projects them all away, so the set would be
-- computed over zero columns: a wrong result and a num_srcs > 0 abort. The pushdown must be skipped
-- unless the set key is preserved. https://github.com/ClickHouse/ClickHouse/issues/110113
SELECT 'count except', count() FROM (SELECT c0 FROM ((SELECT 'a') EXCEPT ALL (SELECT NULL))(c0)) AS t0 WHERE t0.c0 = t0.c0;
SELECT 'count intersect', count() FROM (SELECT c0 FROM ((SELECT 'a') INTERSECT ALL (SELECT 'a'))(c0)) AS t0 WHERE t0.c0 = t0.c0;
DROP TABLE IF EXISTS t_intex_cnt_l;
DROP TABLE IF EXISTS t_intex_cnt_r;
CREATE TABLE t_intex_cnt_l (a UInt64) ENGINE = Memory;
CREATE TABLE t_intex_cnt_r (a UInt64) ENGINE = Memory;
INSERT INTO t_intex_cnt_l VALUES (5),(5),(7);
INSERT INTO t_intex_cnt_r VALUES (5);
SELECT 'count except mt', count() FROM (SELECT a FROM t_intex_cnt_l EXCEPT ALL SELECT a FROM t_intex_cnt_r) WHERE a = 5;
SELECT 'count intersect mt', count() FROM (SELECT a FROM t_intex_cnt_l INTERSECT ALL SELECT a FROM t_intex_cnt_r) WHERE a = 5;
DROP TABLE t_intex_cnt_l;
DROP TABLE t_intex_cnt_r;

-- Decimal plus/minus/multiply raise DECIMAL_OVERFLOW under decimal_check_overflow (default on) but
-- do not advertise it via isSuitableForShortCircuitArgumentsExecution, so the predicate-cannot-throw
-- guard must not rely on that signal. The overflow row (3000000000) is removed by EXCEPT before the
-- top filter runs, so the query returns 1; pushing the multiply into the branch would overflow on the
-- eliminated row. Both directions must agree.
DROP TABLE IF EXISTS t_intex_dec_l;
DROP TABLE IF EXISTS t_intex_dec_r;
CREATE TABLE t_intex_dec_l (a Decimal64(0)) ENGINE = Memory;
CREATE TABLE t_intex_dec_r (a Decimal64(0)) ENGINE = Memory;
INSERT INTO t_intex_dec_l VALUES (1),(3000000000);
INSERT INTO t_intex_dec_r VALUES (3000000000);
SELECT 'dec except on', count() FROM (SELECT a FROM t_intex_dec_l EXCEPT ALL SELECT a FROM t_intex_dec_r) WHERE a * toDecimal64(4000000000, 0) = toDecimal64(4000000000, 0) SETTINGS query_plan_filter_push_down = 1;
SELECT 'dec except off', count() FROM (SELECT a FROM t_intex_dec_l EXCEPT ALL SELECT a FROM t_intex_dec_r) WHERE a * toDecimal64(4000000000, 0) = toDecimal64(4000000000, 0) SETTINGS query_plan_filter_push_down = 0;
DROP TABLE t_intex_dec_l;
DROP TABLE t_intex_dec_r;

-- IntersectOrExcept compares whole branch rows, so a pushed filter must feed the original branch
-- columns into the set, not a computed predicate result. When a parent reuses the predicate column
-- (SELECT x > 0 ... WHERE x > 0) the filter output can be a single same-typed UInt8 (x > 0); pushing
-- it would replace the set key x with x > 0 and change the result. The pushdown must be skipped.
DROP TABLE IF EXISTS t_intex_reuse_l;
DROP TABLE IF EXISTS t_intex_reuse_r;
CREATE TABLE t_intex_reuse_l (x UInt8) ENGINE = Memory;
CREATE TABLE t_intex_reuse_r (x UInt8) ENGINE = Memory;
INSERT INTO t_intex_reuse_l VALUES (2),(2),(5);
INSERT INTO t_intex_reuse_r VALUES (3);
SELECT 'reuse on', count() FROM (SELECT (x > 0) AS p FROM (SELECT x FROM t_intex_reuse_l EXCEPT ALL SELECT x FROM t_intex_reuse_r) WHERE x > 0) SETTINGS query_plan_filter_push_down = 1;
SELECT 'reuse off', count() FROM (SELECT (x > 0) AS p FROM (SELECT x FROM t_intex_reuse_l EXCEPT ALL SELECT x FROM t_intex_reuse_r) WHERE x > 0) SETTINGS query_plan_filter_push_down = 0;
DROP TABLE t_intex_reuse_l;
DROP TABLE t_intex_reuse_r;
