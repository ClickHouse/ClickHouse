-- Tags: shard

-- The result type of a function over LowCardinality arguments is LowCardinality only when
-- at most one argument is a non-constant LowCardinality column; constant arguments are not
-- counted. Constness, however, is not preserved when blocks travel between servers, so at
-- execution a function can receive more non-constant LowCardinality columns than its result
-- type was derived for. This used to throw the LOGICAL_ERROR exception "Default functions
-- implementation for LowCardinality is supported only with a single LowCardinality argument"
-- (during query planning, before reading any data). Now the extra LowCardinality columns are
-- materialized and the result matches local execution.
--
-- Every element of the query is essential to break that invariant:
-- * concatAssumeInjective: for an injective f, GROUP BY f(x, y) is rewritten into
--   GROUP BY x, y and f is re-executed on top of the aggregation
--   (goes away with optimize_injective_functions_in_group_by = 0).
-- * (SELECT toLowCardinality('p')): a constant of a LowCardinality type. As a constant it is
--   not counted for the result type, so the result type stays LowCardinality; but unlike an
--   ordinary-typed constant it is not folded into a literal
--   (goes away with enable_scalar_subquery_optimization = 0), and therefore it is not
--   eliminated from the GROUP BY keys either - it stays a live aggregation key
--   (compare: with (SELECT 'p') the constant key is removed and nothing fails).
-- * Two shards: the initiator has to merge partially aggregated streams, and the merged key
--   columns are always ordinary (non-constant) columns, so the scalar key loses its
--   constness there (goes away with a single-address remote()).
-- * Re-executing concatAssumeInjective over the merged keys then feeds two non-constant
--   LowCardinality columns into a function whose declared result type is LowCardinality.

CREATE TABLE t (s LowCardinality(String)) ENGINE = Memory;
INSERT INTO t VALUES ('x'), ('y'), ('x');

SELECT concatAssumeInjective((SELECT toLowCardinality('p')), s) AS type, count()
FROM remote('127.0.0.{1,2}', currentDatabase(), t)
GROUP BY type
ORDER BY type;
