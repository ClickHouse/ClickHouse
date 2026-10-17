-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

SELECT 'Test 1: latencyValuesAndWeights basic';
WITH latencyValuesAndWeights(serializedQuantiles(number)) AS j
SELECT length(JSONExtractArrayRaw(j, 'values')) > 0,
       length(JSONExtractArrayRaw(j, 'values')) = length(JSONExtractArrayRaw(j, 'weights'))
FROM numbers(1000);

SELECT 'Test 2: latencyValuesAndWeights empty sketch';
SELECT latencyValuesAndWeights(mergeSerializedQuantiles(sketch)) = '{"values":[],"weights":[]}'
FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(0));

SELECT 'Test 3: latencyValuesAndWeights invalid sketch throws';
SELECT latencyValuesAndWeights('invalid'); -- { serverError INCORRECT_DATA }
