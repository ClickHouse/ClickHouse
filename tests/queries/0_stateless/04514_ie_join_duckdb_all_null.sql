-- Tags: no-old-analyzer

-- Empty and all-NULL inputs with `a.x BETWEEN b.x AND b.x` must not read out of bounds.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS test6861;
DROP TABLE IF EXISTS all_null;

CREATE TABLE test6861 (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT * FROM test6861 AS a JOIN test6861 AS b ON a.x BETWEEN b.x AND b.x) WHERE explain LIKE '%IEJoin%';

-- Empty inputs
SELECT count() FROM test6861 AS a JOIN test6861 AS b ON a.x BETWEEN b.x AND b.x;

INSERT INTO test6861 VALUES (1), (2), (3), (NULL), (NULL), (NULL);
CREATE TABLE all_null ENGINE = MergeTree ORDER BY tuple() AS SELECT CAST(NULL, 'Nullable(Int32)') AS x FROM numbers(6);

SELECT count() FROM all_null AS a JOIN all_null AS b ON a.x BETWEEN b.x AND b.x;
SELECT count() FROM test6861 AS a JOIN all_null AS b ON a.x BETWEEN b.x AND b.x;
SELECT count() FROM all_null AS a JOIN test6861 AS b ON a.x BETWEEN b.x AND b.x;

-- The non-NULL values do match themselves
SELECT a.x, b.x FROM test6861 AS a JOIN test6861 AS b ON a.x BETWEEN b.x AND b.x ORDER BY ALL;

DROP TABLE test6861;
DROP TABLE all_null;
