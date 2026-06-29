-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

SELECT 'Test 1: latencyValuesAndWeights basic';
SELECT latencyValuesAndWeights(serializedQuantiles(number)) != '{}' FROM numbers(1000);

SELECT 'Test 2: latencyValuesAndWeights empty sketch';
SELECT latencyValuesAndWeights(mergeSerializedQuantiles(sketch)) = '{}'
FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(0));

SELECT 'Test 3: latencyValuesAndWeights invalid sketch returns {}';
SELECT latencyValuesAndWeights('invalid') = '{}';
