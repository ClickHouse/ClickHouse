-- The `UNIQUE` predicate is parsed into the internal function `__unique`. Formatting must print it back
-- as the `UNIQUE` keyword, so `SHOW CREATE`, `EXPLAIN SYNTAX`, `formatQuery` and persisted view metadata
-- never leak the internal spelling.

SET enable_analyzer = 1;

SELECT formatQuery('SELECT UNIQUE(SELECT 1)');
SELECT formatQuerySingleLine('SELECT UNIQUE(SELECT number FROM numbers(3) UNION ALL SELECT 1), 42');
SELECT formatQuerySingleLine('SELECT UNIQUE((SELECT 1 UNION ALL SELECT 1)) AS u');
SELECT formatQuerySingleLine('SELECT __unique((SELECT 1))');
SELECT formatQuerySingleLine('SELECT if(UNIQUE(SELECT 1), 1, 2), NOT UNIQUE(SELECT 1)');

EXPLAIN SYNTAX SELECT UNIQUE(SELECT number FROM numbers(3));

DROP TABLE IF EXISTS test_unique_format_view;
CREATE VIEW test_unique_format_view AS SELECT UNIQUE(SELECT number FROM numbers(3)) AS u, UNIQUE(SELECT 1 UNION ALL SELECT 1) AS d;
SHOW CREATE VIEW test_unique_format_view;
SELECT * FROM test_unique_format_view;

-- The persisted metadata must be parseable back into the same query.
DETACH TABLE test_unique_format_view;
ATTACH TABLE test_unique_format_view;
SHOW CREATE VIEW test_unique_format_view;
SELECT * FROM test_unique_format_view;
DROP TABLE test_unique_format_view;
