SET enable_analyzer = 1;

-- The `only_analyze` placeholder of the UNIQUE predicate is a fabricated boolean, so it must not be
-- observable by outer constant folding or branch pruning. Without the `materialize` wrapper,
-- `CREATE VIEW` validation (an `only_analyze` consumer) would fold `intDiv(1, UNIQUE(...))` to
-- `intDiv(1, 0)` and throw division by zero even though the executed query is valid, and the `if`
-- and `multiIf` fast paths would prune a branch by the placeholder, hiding an error in the branch
-- that actually runs.

SELECT 'UNIQUE inside a value-sensitive expression under CREATE VIEW';
DROP VIEW IF EXISTS v_unique_no_fold;
CREATE VIEW v_unique_no_fold AS SELECT intDiv(1, UNIQUE((SELECT number FROM numbers(3)))) AS u;
SELECT * FROM v_unique_no_fold;
DROP VIEW v_unique_no_fold;

SELECT 'UNIQUE as a conditional under CREATE VIEW';
DROP VIEW IF EXISTS v_unique_no_prune;
CREATE VIEW v_unique_no_prune AS SELECT if(UNIQUE((SELECT number FROM numbers(3))), 'unique', 'duplicate') AS u;
SELECT * FROM v_unique_no_prune;
DROP VIEW v_unique_no_prune;

SELECT 'the false branch still executes correctly';
DROP VIEW IF EXISTS v_unique_dup;
CREATE VIEW v_unique_dup AS SELECT if(UNIQUE((SELECT 1 UNION ALL SELECT 1)), 'unique', 'duplicate') AS u;
SELECT * FROM v_unique_dup;
DROP VIEW v_unique_dup;

SELECT 'EXPLAIN does not fold the placeholder';
SELECT count() >= 1 FROM (EXPLAIN QUERY TREE SELECT intDiv(1, UNIQUE((SELECT number FROM numbers(3)))));

SELECT 'the placeholder does not prune a branch of if';
DROP VIEW IF EXISTS v_unique_if_prune;
-- The predicate is false at execution time, so the `else` branch is the one that runs. A truthy
-- placeholder must not let `CREATE VIEW` validation prune it and swallow its `UNKNOWN_IDENTIFIER`.
CREATE VIEW v_unique_if_prune AS SELECT if(UNIQUE((SELECT 1 UNION ALL SELECT 1)), 1, no_such_column) AS u; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'the placeholder does not prune a branch of multiIf';
DROP VIEW IF EXISTS v_unique_multi_if_prune;
CREATE VIEW v_unique_multi_if_prune AS SELECT multiIf(UNIQUE((SELECT 1 UNION ALL SELECT 1)), 1, 1, 2, no_such_column) AS u; -- { serverError UNKNOWN_IDENTIFIER }

-- `EXPLAIN QUERY TREE` evaluates the predicate for real, so the branch it keeps is decided by the
-- true value and not by a placeholder. The unreachable branch is then genuinely unreachable.
SELECT 'the same holds for EXPLAIN';
EXPLAIN QUERY TREE SELECT if(UNIQUE((SELECT 1 UNION ALL SELECT 1)), 1, no_such_column); -- { serverError UNKNOWN_IDENTIFIER }
