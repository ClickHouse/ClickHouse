-- `obfuscate` rejects an output type its models cannot handle. The check belongs to the query plan,
-- not to the table structure: the structure is the whole inner query, while a read builds a model only
-- for the columns it selects, so a column of an unsupported type that is projected away must not make
-- the invocation unusable. `DESCRIBE` therefore reports such a column, a read of the supported columns
-- works, and only a read of the unsupported one is rejected.

DESCRIBE obfuscate(SELECT number AS n, toLowCardinality('x') AS s FROM numbers(10));
SELECT count() FROM (SELECT n FROM obfuscate(SELECT number AS n, toLowCardinality('x') AS s FROM numbers(10)) LIMIT 3);
SELECT s FROM obfuscate(SELECT number AS n, toLowCardinality('x') AS s FROM numbers(10)) LIMIT 1; -- { serverError NOT_IMPLEMENTED }

-- The rejection happens while the plan is built, so it does not depend on the query reaching execution.
EXPLAIN PLAN SELECT s FROM obfuscate(SELECT toLowCardinality('x') AS s FROM numbers(10)) LIMIT 1; -- { serverError NOT_IMPLEMENTED }
EXPLAIN PIPELINE SELECT s FROM obfuscate(SELECT toLowCardinality('x') AS s FROM numbers(10)) LIMIT 1; -- { serverError NOT_IMPLEMENTED }

-- A query-level `SETTINGS` clause of a `view` is not visible while the table structure is derived, but
-- it is applied when the view is read, so the `obfuscate_*` settings must not be validated at structure
-- derivation time either.
SET obfuscate_markov_order = 0;
DESCRIBE view(SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_markov_order = DEFAULT);
SELECT count() FROM view(SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_markov_order = DEFAULT);
