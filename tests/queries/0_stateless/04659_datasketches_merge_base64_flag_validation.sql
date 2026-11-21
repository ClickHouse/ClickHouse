-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- The base64_encoded parameter is documented as a 0/1 flag. Any other value must be
-- rejected instead of being silently treated as true.

SELECT mergeSerializedQuantiles(2)(sketch) FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedTDigest(2)(sketch) FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedQuantiles(42)(sketch) FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mergeSerializedTDigest(42)(sketch) FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The valid values still work.
SELECT length(mergeSerializedQuantiles(0)(sketch)) > 0 FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(10));
SELECT length(mergeSerializedQuantiles(1)(sketch)) > 0 FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(10));
SELECT length(mergeSerializedTDigest(0)(sketch)) > 0 FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(10));
SELECT length(mergeSerializedTDigest(1)(sketch)) > 0 FROM (SELECT serializedTDigest(number) AS sketch FROM numbers(10));
