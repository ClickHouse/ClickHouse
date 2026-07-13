-- Tags: stateful, replica

SELECT count() > 0 FROM (SELECT ParsedParams.Key1 AS p FROM test.visits WHERE arrayAll(y -> arrayExists(x -> y != x, p), p))
