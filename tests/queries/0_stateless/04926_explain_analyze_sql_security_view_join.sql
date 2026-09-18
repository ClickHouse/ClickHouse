-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- `EXPLAIN ANALYZE` must reach the joins inside a view whose SQL security rebuilds the context
-- (`DEFINER` and `NONE` start from the global context). Under `hash` the `Left` group is reported
-- only when the join was told to collect statistics, so a zero count means the mode did not arrive.

SET enable_analyzer = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS r_join;
DROP VIEW IF EXISTS v_definer;
DROP VIEW IF EXISTS v_none;
DROP VIEW IF EXISTS v_invoker;
DROP VIEW IF EXISTS v_definer_filled;

CREATE TABLE l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO l SELECT number, number FROM numbers(100);
INSERT INTO r SELECT number, number FROM numbers(50);

CREATE TABLE r_join (k UInt64, w UInt64) ENGINE = Join(ALL, LEFT, k);
INSERT INTO r_join SELECT number, number FROM numbers(50);

CREATE VIEW v_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT l.v AS v, r.w AS w FROM l LEFT JOIN r ON l.k = r.k;
CREATE VIEW v_none SQL SECURITY NONE AS
    SELECT l.v AS v, r.w AS w FROM l LEFT JOIN r ON l.k = r.k;
CREATE VIEW v_invoker SQL SECURITY INVOKER AS
    SELECT l.v AS v, r.w AS w FROM l LEFT JOIN r ON l.k = r.k;
CREATE VIEW v_definer_filled DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT l.v AS v, r_join.w AS w FROM l LEFT JOIN r_join ON l.k = r_join.k;

-- JoinStep, DEFINER: the reported shape.
SELECT countIf(explain LIKE '%Left: rows%') = 1
FROM (EXPLAIN ANALYZE SELECT v, w FROM v_definer);

-- JoinStep, NONE: the same context rebuild.
SELECT countIf(explain LIKE '%Left: rows%') = 1
FROM (EXPLAIN ANALYZE SELECT v, w FROM v_none);

-- FilledJoinStep, DEFINER: the join against an `ENGINE = Join` table.
SELECT countIf(explain LIKE '%Left: rows%') = 1
FROM (EXPLAIN ANALYZE SELECT v, w FROM v_definer_filled);

-- INVOKER keeps the caller's context, so it was already reporting statistics.
SELECT countIf(explain LIKE '%Left: rows%') = 1
FROM (EXPLAIN ANALYZE SELECT v, w FROM v_invoker);

DROP VIEW v_definer_filled;
DROP VIEW v_invoker;
DROP VIEW v_none;
DROP VIEW v_definer;
DROP TABLE r_join;
DROP TABLE r;
DROP TABLE l;
