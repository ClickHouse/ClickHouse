-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_alias;

CREATE TABLE t_push_subcolumns_alias
(
    id UInt32,
    tup Tuple(a UInt32, b String),
    t1 ALIAS tup,
    t2 ALIAS t1,
    computed ALIAS tuple(tup.a + 1, tup.b)::Tuple(a UInt32, b String)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_alias (id, tup) VALUES (1, (1, 'one')), (2, (2, 'two'));

SELECT 'ALIAS column exported by a subquery';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT t1.a FROM (SELECT t1 FROM t_push_subcolumns_alias)) WHERE explain LIKE '%Output%';
SELECT t1.a FROM (SELECT t1 FROM t_push_subcolumns_alias) ORDER BY t1.a;

SELECT 'chained ALIAS column exported by a subquery';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT t2.b FROM (SELECT t2 FROM t_push_subcolumns_alias)) WHERE explain LIKE '%Output%';
SELECT t2.b FROM (SELECT t2 FROM t_push_subcolumns_alias) ORDER BY t2.b;

SELECT 'ALIAS column exported by a view';
DROP TABLE IF EXISTS v_push_subcolumns_alias;
CREATE VIEW v_push_subcolumns_alias AS SELECT id, t2 FROM t_push_subcolumns_alias;
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT t2.a FROM v_push_subcolumns_alias) WHERE explain LIKE '%Output%';
SELECT t2.a FROM v_push_subcolumns_alias ORDER BY t2.a;
DROP TABLE v_push_subcolumns_alias;

SELECT 'non-trivial ALIAS is not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT computed.a FROM (SELECT computed FROM t_push_subcolumns_alias)) WHERE explain LIKE '%Output%';
SELECT computed.a FROM (SELECT computed FROM t_push_subcolumns_alias) ORDER BY computed.a;

DROP TABLE t_push_subcolumns_alias;
