-- With the old analyzer, `ApplyWithAliasVisitor` propagates every visible `WITH` alias by cloning it
-- into every descendant subquery. It is meant to run once, on a query that is not itself a subquery -
-- but an alias whose subquery is interpreted as a fresh query is analysed by a new non-subquery
-- interpreter, which applies the propagation again to an AST that already carries the injected aliases.
-- `k` such aliases therefore compound to `k ^ d` nodes over `d` levels, and three were already enough to
-- exhaust the server's memory rather than fail. The expansion is now bounded like alias substitution in
-- `QueryNormalizer`. A plain nested subquery is interpreted as a subquery and does not compound.

SET enable_analyzer = 0;

-- Three aliases: the expansion crosses `max_expanded_ast_elements` and must be reported, not allocated.
WITH (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e1,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e2,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e3
SELECT e1, e2, e3; -- { serverError TOO_BIG_AST }

-- Not specific to `EXPLAIN`: `view()` is interpreted the same way and compounds identically.
WITH (SELECT count() FROM view(SELECT 1)) AS e1,
     (SELECT count() FROM view(SELECT 1)) AS e2,
     (SELECT count() FROM view(SELECT 1)) AS e3
SELECT e1, e2, e3; -- { serverError TOO_BIG_AST }

-- `EXPLAIN AST` does not interpret its argument, so it does not compound.
SELECT 'explain ast unaffected';
WITH (SELECT count() FROM (EXPLAIN AST SELECT 1)) AS e1,
     (SELECT count() FROM (EXPLAIN AST SELECT 1)) AS e2,
     (SELECT count() FROM (EXPLAIN AST SELECT 1)) AS e3
SELECT e1 > 0, e2 > 0, e3 > 0;

-- Below the limit the query is unaffected.
SELECT 'two aliases still work';
WITH (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e1,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e2
SELECT e1 > 0, e2 > 0;

-- A plain alias is not affected by the bound at all.
SELECT 'plain aliases unaffected';
WITH (SELECT count() FROM numbers(10)) AS e1,
     (SELECT count() FROM numbers(10)) AS e2,
     (SELECT count() FROM numbers(10)) AS e3
SELECT e1, e2, e3;

-- The parallel distributed INSERT ... SELECT fast path applies the bound before the source SELECT's
-- own SETTINGS have been applied, and the parser's push-down onto the INSERT does not overwrite a
-- setting the INSERT already carries. The source SELECT's value must still win, as it does on the
-- ordinary path.
DROP TABLE IF EXISTS t_with_alias_repl;
DROP TABLE IF EXISTS t_with_alias_dist;

CREATE TABLE t_with_alias_repl (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_with_alias', '1') ORDER BY a;
CREATE TABLE t_with_alias_dist AS t_with_alias_repl ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_with_alias_repl);

SELECT 'source SETTINGS win on the insert fast path';
INSERT INTO t_with_alias_dist SETTINGS parallel_distributed_insert_select = 2, max_expanded_ast_elements = 200
WITH (SELECT count() FROM (SELECT 1)) AS e1,
     (SELECT count() FROM (SELECT 1)) AS e2,
     (SELECT count() FROM (SELECT 1)) AS e3
SELECT e1 + e2 + e3
SETTINGS max_expanded_ast_elements = 100000000000;

-- Only that the INSERT was accepted matters; counting the rows back would race with the distributed
-- insert becoming visible.
SELECT 'insert accepted';

-- The fast path runs the old interpreter's alias propagation whatever the analyzer setting is, so the
-- bound must not apply when the analyzer is on: there the visitor makes a single pass, and every other
-- path accepts this query.
SELECT 'analyzer is not bounded on the insert fast path';
INSERT INTO t_with_alias_dist SETTINGS parallel_distributed_insert_select = 2, enable_analyzer = 1, max_expanded_ast_elements = 100
WITH (SELECT count() FROM (SELECT 1)) AS e1,
     (SELECT count() FROM (SELECT 1)) AS e2,
     (SELECT count() FROM (SELECT 1)) AS e3
SELECT e1 + e2 + e3;
SELECT 'analyzer insert accepted';

DROP TABLE t_with_alias_dist;
DROP TABLE t_with_alias_repl;

-- The analyzer does not clone aliases per subquery and handles all of it.
SELECT 'analyzer unaffected';
WITH (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e1,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e2,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e3
SELECT e1 > 0, e2 > 0, e3 > 0 SETTINGS enable_analyzer = 1;
