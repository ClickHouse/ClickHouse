SET enable_json_type = 1;
SET allow_suspicious_types_in_group_by = 1;

-- Neither JSON nor Array(JSON) has a poolable serialization, so a Dynamic holding one is the only
-- shape whose arena serialization is resolved per call instead of being read from VariantInfo.
-- Aggregating over two keys selects the serialized method, which serializes the Dynamic key per row.

SELECT dynamicType(d), toString(d), m, count()
FROM (SELECT toJSONString(map('k', toString(number % 3)))::JSON::Dynamic AS d, number % 2 AS m FROM numbers(30))
GROUP BY d, m
ORDER BY toString(d), m;

SELECT dynamicType(d), toString(d), m, count()
FROM (SELECT [toJSONString(map('k', toString(number % 3)))::JSON]::Array(JSON)::Dynamic AS d, number % 2 AS m FROM numbers(30))
GROUP BY d, m
ORDER BY toString(d), m;

-- groupUniqArray keys its own hash set by the same arena bytes, and those bytes are its persisted
-- aggregate state, so this reaches the serialization independently of the aggregation key path.
SELECT arraySort(arrayMap(x -> toString(x), groupUniqArray(d)))
FROM (SELECT toJSONString(map('k', toString(number % 3)))::JSON::Dynamic AS d FROM numbers(30));
