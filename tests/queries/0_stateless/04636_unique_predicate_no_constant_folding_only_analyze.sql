SET enable_analyzer = 1;

-- The `only_analyze` placeholder of the UNIQUE predicate is a fabricated boolean, so it must not be
-- observable by outer constant folding or branch pruning. Without the `__scalarSubqueryResult`
-- wrapper, `CREATE VIEW` validation (an `only_analyze` consumer) would fold `intDiv(1, UNIQUE(...))`
-- to `intDiv(1, 0)` and throw division by zero, even though the executed query is valid because the
-- subquery is unique and the predicate evaluates to 1.

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
