-- A value in the shared variant of a `Dynamic` column can have a `LowCardinality` type.
-- Negative zero in such a value has to be treated as equal to positive zero, like `equals` does.

SET allow_suspicious_low_cardinality_types = 1;
SET allow_suspicious_types_in_group_by = 1;

SELECT dynamicType(x), isDynamicElementInSharedData(x)
FROM (SELECT arrayJoin([CAST(toLowCardinality(0.0), 'Dynamic(max_types=0)'), CAST(toLowCardinality(-0.0), 'Dynamic(max_types=0)')]) AS x);

SELECT length(groupUniqArray(x)), length(groupArrayDistinct(x)), length(topK(10)(x))
FROM (SELECT arrayJoin([CAST(toLowCardinality(0.0), 'Dynamic(max_types=0)'), CAST(toLowCardinality(-0.0), 'Dynamic(max_types=0)')]) AS x);

SELECT count() FROM (SELECT arrayJoin([CAST(toLowCardinality(0.0), 'Dynamic(max_types=0)'), CAST(toLowCardinality(-0.0), 'Dynamic(max_types=0)')]) AS x GROUP BY x);
SELECT count() FROM (SELECT DISTINCT arrayJoin([CAST(toLowCardinality(0.0), 'Dynamic(max_types=0)'), CAST(toLowCardinality(-0.0), 'Dynamic(max_types=0)')]) AS x);

SELECT 'nested';
SELECT count() FROM (SELECT DISTINCT arrayJoin([CAST([toLowCardinality(0.0), 1], 'Dynamic(max_types=0)'), CAST([toLowCardinality(-0.0), 1], 'Dynamic(max_types=0)')]) AS x);

SELECT 'different values are still distinct';
SELECT count() FROM (SELECT DISTINCT arrayJoin([CAST(toLowCardinality(0.0), 'Dynamic(max_types=0)'), CAST(toLowCardinality(-0.0), 'Dynamic(max_types=0)'), CAST(toLowCardinality(1.0), 'Dynamic(max_types=0)'), CAST(0.0, 'Dynamic(max_types=0)')]) AS x);
