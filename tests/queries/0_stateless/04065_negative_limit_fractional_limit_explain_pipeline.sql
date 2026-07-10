-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings
-- EXPLAIN output may differ

-- { echo }

-- Negative limit
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -3) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Negative limit with ties
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -3 WITH TIES) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Negative limit + negative offset
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -2, -3) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Negative limit + negative offset with ties
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -2, -3 WITH TIES) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Negative limit + positive offset
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 2, -3) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Negative limit + positive offset with ties
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 2, -3 WITH TIES) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Positive limit + negative offset
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -2, 3) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Positive limit + negative offset with ties
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -2, 3 WITH TIES) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Fractional limit
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 0.5) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Fractional limit with ties
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 0.5 WITH TIES) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Fractional limit with offset
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 0.3 OFFSET 0.2) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';
-- Fractional limit with offset and ties
SELECT explain FROM (EXPLAIN actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 0.3 OFFSET 0.2 WITH TIES) WHERE explain LIKE '%Negative%' OR explain LIKE '%WITH TIES%' OR explain LIKE '%Fractional%';

-- JSON format: negative limit
SELECT explain LIKE '%"Negative Limit": 3%' AND explain LIKE '%"Negative Offset": 0%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -3);
-- JSON format: negative limit with ties
SELECT explain LIKE '%"Negative Limit": 3%' AND explain LIKE '%"Negative Offset": 0%' AND explain LIKE '%"With Ties": true%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -3 WITH TIES);
-- JSON format: negative limit + negative offset
SELECT explain LIKE '%"Negative Limit": 3%' AND explain LIKE '%"Negative Offset": 2%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -2, -3);
-- JSON format: negative limit + negative offset with ties
SELECT explain LIKE '%"Negative Limit": 3%' AND explain LIKE '%"Negative Offset": 2%' AND explain LIKE '%"With Ties": true%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -2, -3 WITH TIES);
-- JSON format: negative limit + positive offset
SELECT explain LIKE '%"Negative Limit": 3%' AND explain LIKE '%"Negative Offset": 0%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 2, -3);
-- JSON format: negative limit + positive offset with ties
SELECT explain LIKE '%"Negative Limit": 3%' AND explain LIKE '%"Negative Offset": 0%' AND explain LIKE '%"With Ties": true%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 2, -3 WITH TIES);
-- JSON format: positive limit + negative offset
SELECT explain LIKE '%"Negative Offset": 2%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -2, 3);
-- JSON format: positive limit + negative offset with ties
SELECT explain LIKE '%"Negative Offset": 2%' AND explain LIKE '%"With Ties": true%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT -2, 3 WITH TIES);
-- JSON format: fractional limit
SELECT explain LIKE '%"Fractional Limit": 0.5%' AND explain LIKE '%"Fractional Offset": 0%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 0.5);
-- JSON format: fractional limit with ties
SELECT explain LIKE '%"Fractional Limit": 0.5%' AND explain LIKE '%"Fractional Offset": 0%' AND explain LIKE '%"With Ties": true%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 0.5 WITH TIES);
-- JSON format: fractional limit with offset
SELECT explain LIKE '%"Fractional Limit": 0.3%' AND explain LIKE '%"Fractional Offset": 0.2%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 0.3 OFFSET 0.2);
-- JSON format: fractional limit with offset and ties
SELECT explain LIKE '%"Fractional Limit": 0.3%' AND explain LIKE '%"Fractional Offset": 0.2%' AND explain LIKE '%"With Ties": true%' FROM (EXPLAIN json=1, actions=1 SELECT number FROM numbers(10) ORDER BY number LIMIT 0.3 OFFSET 0.2 WITH TIES);
