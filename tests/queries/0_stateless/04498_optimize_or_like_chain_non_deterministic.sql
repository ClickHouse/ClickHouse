-- `optimize_or_like_chain` must not merge LIKE branches whose first argument contains a
-- non-deterministic function (e.g. `rand`). Both branches render to the same alias/column
-- name, but at runtime they evaluate independently — collapsing two `rand()` calls into
-- a single `multiSearchAny` would change query results. `optimize_or_like_chain` is a
-- new-analyzer-only optimization, so all cases use the analyzer (`enable_analyzer = 1`).

SET optimize_or_like_chain = 1;
-- Exercise the rewrite even for short chains (the default thresholds would skip 2-pattern fixtures).
SET optimize_or_like_chain_min_patterns = 1;
SET optimize_or_like_chain_min_substrings = 1;

-- Non-deterministic LHS (`rand`): the rewrite must keep two separate LIKE expressions.
EXPLAIN QUERY TREE run_passes = 1
SELECT *
FROM (SELECT 1 AS x)
WHERE (toString(rand()) LIKE '%1%') OR (toString(rand()) LIKE '%2%')
SETTINGS enable_analyzer = 1;

-- A pure-deterministic chain on the same column must still be merged (into `multiSearchAny`).
DROP TABLE IF EXISTS t_or_like_chain_nondet;
CREATE TABLE t_or_like_chain_nondet (s String) ENGINE = Memory;
INSERT INTO t_or_like_chain_nondet VALUES ('foo');

EXPLAIN QUERY TREE run_passes = 1
SELECT * FROM t_or_like_chain_nondet
WHERE (s LIKE '%a%') OR (s LIKE '%b%')
SETTINGS enable_analyzer = 1;

DROP TABLE t_or_like_chain_nondet;

-- A non-deterministic call nested inside a `LambdaNode` body (here `rand()` inside `arrayMap`)
-- must also block the rewrite. Without descending past `LambdaNode`, the check would treat the
-- two structurally identical LHS expressions as deterministic and collapse them into a single
-- `multiSearchAny`, calling `rand` once instead of twice.
EXPLAIN QUERY TREE run_passes = 1
SELECT *
FROM (SELECT 1 AS x)
WHERE arrayElement(arrayMap(x -> toString(rand() + x), [1]), 1) LIKE '%1%'
   OR arrayElement(arrayMap(x -> toString(rand() + x), [1]), 1) LIKE '%2%'
SETTINGS enable_analyzer = 1;
