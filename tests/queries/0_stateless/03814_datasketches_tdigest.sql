-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

SELECT 'Test 1: serializedTDigest basic';
SELECT length(serializedTDigest(number)) > 0 FROM numbers(1000);

SELECT 'Test 2: mergeSerializedTDigest basic';
SELECT length(mergeSerializedTDigest(sketch)) > 0
FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(1000));

SELECT 'Test 3: percentileFromTDigest p50';
SELECT percentileFromTDigest(serializedTDigest(number), 0.5) BETWEEN 450 AND 550
FROM numbers(1000);

SELECT 'Test 4: percentileFromTDigest p95';
SELECT percentileFromTDigest(serializedTDigest(number), 0.95) BETWEEN 900 AND 1000
FROM numbers(1000);

SELECT 'Test 5: percentileFromTDigest empty sketch returns NaN';
SELECT isNaN(percentileFromTDigest(mergeSerializedTDigest(sketch), 0.5))
FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(0));

SELECT 'Test 6: percentileFromTDigest invalid sketch throws';
SELECT percentileFromTDigest('invalid', 0.5); -- { serverError INCORRECT_DATA }

SELECT 'Test 7: centroidsFromTDigest basic';
WITH centroidsFromTDigest(serializedTDigest(number)) AS j
SELECT length(JSONExtractArrayRaw(j, 'means')) > 0,
       length(JSONExtractArrayRaw(j, 'means')) = length(JSONExtractArrayRaw(j, 'weights'))
FROM numbers(1000);

SELECT 'Test 8: centroidsFromTDigest empty sketch';
SELECT centroidsFromTDigest(mergeSerializedTDigest(sketch)) = '{"means":[],"weights":[]}'
FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(0));

SELECT 'Test 9: centroidsFromTDigest invalid sketch throws';
SELECT centroidsFromTDigest('invalid'); -- { serverError INCORRECT_DATA }
