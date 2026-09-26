-- Tests that a predicate passed to `mergeTreeAnalyzeIndexes` works when it defines and later
-- references an inline alias (`expr AS a ... a`). The predicate is resolved as a standalone
-- constant expression via `QueryAnalyzer::resolveConstantExpression`, which previously did not
-- collect the expression's internal aliases, so such a predicate failed with UNKNOWN_IDENTIFIER
-- even though the same shape is accepted everywhere else (SELECT, DEFAULT expressions, etc.).

DROP TABLE IF EXISTS t_04514;

CREATE TABLE t_04514 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_04514 SELECT number, number + 1000000 FROM numbers(100000);

-- Sanity check: a plain predicate resolves.
SELECT count() > 0 FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_04514, key = 8193);

-- Inline alias defined and later referenced inside a nested function.
SELECT count() > 0 FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_04514, concat(substring(toString(key) AS h, 1, 1), h) = '11');

-- Alias defined and referenced at the top level of the predicate.
SELECT count() > 0 FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_04514, (key AS a) = a);

DROP TABLE t_04514;
