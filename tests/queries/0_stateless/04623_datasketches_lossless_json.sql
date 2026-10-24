-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- The JSON exporters must not lose data when a sketch legitimately retains
-- the same value multiple times: parallel arrays are used instead of JSON
-- objects keyed by value/mean (object keys would collapse duplicates).

SELECT 'Constant-valued Quantiles sketch keeps all retained entries';
WITH latencyValuesAndWeights(serializedQuantiles(materialize(42.0))) AS j
SELECT length(JSONExtractArrayRaw(j, 'values')) = length(JSONExtractArrayRaw(j, 'weights')),
       arraySum(JSONExtract(j, 'weights', 'Array(UInt64)')) = 1000
FROM numbers(1000);

SELECT 'Constant-valued TDigest sketch keeps all centroids';
WITH centroidsFromTDigest(serializedTDigest(materialize(42.0))) AS j
SELECT length(JSONExtractArrayRaw(j, 'means')) = length(JSONExtractArrayRaw(j, 'weights')),
       arraySum(JSONExtract(j, 'weights', 'Array(Int64)')) = 1000
FROM numbers(1000);
