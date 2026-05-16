-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings
-- EXPLAIN output may differ

-- { echo }

DROP TABLE IF EXISTS test;
CREATE TABLE test (g UInt8, x UInt32) ENGINE = MergeTree ORDER BY (g, x)
AS SELECT number % 3 AS g, number AS x FROM numbers(30);

SELECT replaceRegexpAll(explain, '^\s*', '') FROM (EXPLAIN actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 BY g) WHERE match(explain, '^\s*(NegativeLimitBy|LimitBy|Negative Length|Negative Offset|Length |Offset )');

SELECT replaceRegexpAll(explain, '^\s*', '') FROM (EXPLAIN actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET -3 BY g) WHERE match(explain, '^\s*(NegativeLimitBy|LimitBy|Negative Length|Negative Offset|Length |Offset )');

SELECT replaceRegexpAll(explain, '^\s*', '') FROM (EXPLAIN actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET 3 BY g) WHERE match(explain, '^\s*(NegativeLimitBy|LimitBy|Negative Length|Negative Offset|Length |Offset )');

SELECT replaceRegexpAll(explain, '^\s*', '') FROM (EXPLAIN actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 2 OFFSET -3 BY g) WHERE match(explain, '^\s*(NegativeLimitBy|LimitBy|Negative Length|Negative Offset|Length |Offset )');

SELECT replaceRegexpAll(explain, '^\s*', '') FROM (EXPLAIN actions=1 SELECT number FROM numbers(12) ORDER BY number % 2, number % 3, number LIMIT -1 BY number % 2, number % 3) WHERE match(explain, '^\s*(NegativeLimitBy|LimitBy|Negative Length|Negative Offset|Length |Offset )');

SELECT replaceRegexpAll(explain, '^\s*', '') FROM (EXPLAIN actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT -2 BY g) WHERE match(explain, '^\s*(NegativeLimitBy|LimitBy|Negative Length|Negative Offset|Length |Offset )');

SELECT explain LIKE '%"Node Type": "NegativeLimitBy"%' AND explain LIKE '%"Negative Length": 2%' AND explain LIKE '%"Negative Offset": 3%' FROM (EXPLAIN json=1, actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET -3 BY g);

SELECT explain LIKE '%"Node Type": "NegativeLimitBy"%' AND explain LIKE '%"Negative Length": 2%' AND explain LIKE '%"Negative Offset": 0%' AND explain NOT LIKE '%"Node Type": "LimitBy"%' FROM (EXPLAIN json=1, actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 BY g);

SELECT explain LIKE '%"Node Type": "NegativeLimitBy"%' AND explain LIKE '%"Negative Length": 2%' AND explain LIKE '%"Node Type": "LimitBy"%' AND explain LIKE '%"Offset": 3%' FROM (EXPLAIN json=1, actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET 3 BY g);

SELECT explain LIKE '%"Node Type": "NegativeLimitBy"%' AND explain LIKE '%"Negative Offset": 3%' AND explain LIKE '%"Node Type": "LimitBy"%' AND explain LIKE '%"Length": 2%' FROM (EXPLAIN json=1, actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 2 OFFSET -3 BY g);

SELECT explain LIKE '%"Node Type": "NegativeLimitBy"%' AND explain LIKE '%"Negative Offset": 3%' AND explain LIKE '%"Node Type": "LimitBy"%' AND explain LIKE '%"Length": 0%' FROM (EXPLAIN json=1, actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 0 OFFSET -3 BY g);

SELECT explain LIKE '%"Node Type": "NegativeLimitBy"%' AND explain LIKE '%"Negative Length": 2%' FROM (EXPLAIN json=1, actions=1 SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT -2 BY g);

SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test ORDER BY g, x LIMIT -2 BY g) WHERE explain ILIKE '%NegativeLimitBy%' OR explain ILIKE '%LimitByTransform%';

SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test ORDER BY g DESC, x DESC LIMIT -2 BY g) WHERE explain ILIKE '%NegativeLimitBy%' OR explain ILIKE '%LimitByTransform%';

SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test ORDER BY x LIMIT -2 BY g) WHERE explain ILIKE '%NegativeLimitBy%' OR explain ILIKE '%LimitByTransform%';

SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test LIMIT -2 BY g) WHERE explain ILIKE '%NegativeLimitBy%' OR explain ILIKE '%LimitByTransform%';

SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test ORDER BY g, x LIMIT 2 OFFSET -3 BY g) WHERE explain ILIKE '%NegativeLimitBy%' OR explain ILIKE '%LimitByTransform%';

SELECT DISTINCT trim(BOTH ' ' FROM explain) FROM (EXPLAIN PIPELINE SELECT g, x FROM test ORDER BY x LIMIT 2 OFFSET -3 BY g) WHERE explain ILIKE '%NegativeLimitBy%' OR explain ILIKE '%LimitByTransform%';

DROP TABLE test;
