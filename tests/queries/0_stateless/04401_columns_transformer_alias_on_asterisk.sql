-- Regression test: a column transformer that assigns an alias to its result
-- (APPLY with a name prefix, or REPLACE) crashed with "Can't set alias of ...
-- of QualifiedAsterisk" (LOGICAL_ERROR, server abort in debug/sanitizer builds)
-- when the transformer expression expanded to an asterisk / qualified asterisk /
-- COLUMNS matcher. Now gives a clear BAD_ARGUMENTS error.
-- https://github.com/ClickHouse/ClickHouse/issues/109214

-- Old analyzer: setting a name/alias on an expanded asterisk is rejected cleanly.
SET enable_analyzer = 0;

-- APPLY lambda whose body is a qualified asterisk, with a name prefix (the fuzzer query).
SELECT * APPLY (x -> compound_value.*, 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- APPLY lambda whose body is a plain asterisk, with a name prefix.
SELECT * APPLY (x -> *, 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- APPLY lambda whose body is a COLUMNS matcher, with a name prefix.
SELECT * APPLY (x -> COLUMNS('a'), 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- REPLACE whose replacement expression is a qualified asterisk.
SELECT * REPLACE (compound_value.* AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- REPLACE whose replacement expression is a plain asterisk / COLUMNS matcher.
SELECT * REPLACE (* AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (COLUMNS('a') AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- The same rejection must hold on a multi-column input (not just single-column).
SELECT * APPLY (x -> *, 'f_') FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }
SELECT * APPLY (x -> COLUMNS('a'), 'f_') FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (COLUMNS('a') AS a) FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }

-- Valid transformers must keep working (old analyzer).
SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'p_') APPLY (x -> x + 1, 'q_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * REPLACE (a + 1 AS a) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
-- A matcher nested inside a function is not a bare matcher, so a named APPLY over it is
-- still allowed (only the top-level result being a bare matcher is rejected).
SELECT * APPLY (x -> tuple(*), 'f_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
-- The prefix uses the short column name, not a qualified one, even in a scope that
-- requires qualification (alias `a` collides with `x.a`, so `x.*` qualifies to `x.a`).
SELECT 99 AS a, x.* APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) AS x FORMAT TSVWithNames;
-- A later unprefixed APPLY names its argument from the actual previous expression, not from
-- the earlier prefix alias: prefixed then unprefixed gives `upper(toString(a))`, and a
-- function-form identity chain gives `toString(identity(a))`.
SELECT * APPLY (toString, 'f_') APPLY upper FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (identity, 'p_') APPLY toString FROM (SELECT 1 AS a) FORMAT TSVWithNames;

-- New (default) analyzer: the APPLY name prefix must be applied consistently with the
-- old analyzer (it used to be silently dropped). https://github.com/ClickHouse/ClickHouse/pull/109223
SET enable_analyzer = 1;

-- The default analyzer must honor the same BAD_ARGUMENTS contract as the old analyzer for a
-- named transformer over a bare matcher/asterisk expansion. It used to either accept the
-- query and rename the reused column (one-column input) or emit a generic UNSUPPORTED_METHOD
-- (multi-column input) instead of rejecting it.
SELECT * APPLY (x -> compound_value.*, 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * APPLY (x -> *, 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * APPLY (x -> COLUMNS('a'), 'f_') FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (compound_value.* AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (* AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (COLUMNS('a') AS a) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
-- Multi-column input must be rejected with the same BAD_ARGUMENTS (previously UNSUPPORTED_METHOD).
SELECT * APPLY (x -> *, 'f_') FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }
SELECT * APPLY (x -> COLUMNS('a'), 'f_') FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }
SELECT * REPLACE (COLUMNS('a') AS a) FROM (SELECT 1 AS a, 2 AS b); -- { serverError BAD_ARGUMENTS }
-- A matcher nested inside a function is not a bare matcher: still allowed, matching the old analyzer.
SELECT * APPLY (x -> tuple(*), 'f_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;

SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'f_') FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> x + 1, 'p_') APPLY (x -> x + 1, 'q_') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * REPLACE (a + 1 AS a) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
-- The prefix uses the short column name (`f_a`), not the qualified projection name
-- (`f_x.a`) that qualifyColumnNodesWithProjectionNames stores in a qualifying scope.
SELECT 99 AS a, x.* APPLY (toString, 'f_') FROM (SELECT 1 AS a, 2 AS b) AS x FORMAT TSVWithNames;
-- A later unprefixed APPLY must format its argument from the actual previous expression,
-- matching the old analyzer: `upper(toString(a))` (not `upper(f_a)`), and a function-form
-- identity chain gives `toString(identity(a))` (not `toString(p_a)`). The prefix only sets
-- this column's own terminal display name; it must not leak into a chained transformer's
-- argument name for a freshly created node.
SELECT * APPLY (toString, 'f_') APPLY upper FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (identity, 'p_') APPLY toString FROM (SELECT 1 AS a) FORMAT TSVWithNames;
-- An identity lambda (`x -> x`) instead resolves back to the original matched node, which
-- has no old-analyzer equivalent; the accumulated prefix is carried onto the reused node so
-- a chained transformer observes it (`toString(p_a)`). This overwrites (not just emplaces)
-- both node_to_projection_name and the resolved-expression cache.
SELECT * APPLY (x -> x, 'p_') APPLY toString FROM (SELECT 1 AS a) FORMAT TSVWithNames;
-- The prefix survives an EXPLAIN QUERY TREE round-trip (toAST reconstructs it).
SELECT count() FROM (EXPLAIN QUERY TREE SELECT * APPLY (toString, 'f_') FROM (SELECT 1 AS a)) WHERE explain ILIKE '%f_a%';
