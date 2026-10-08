-- The aggregate functions that deduplicate the serialized values of a `Dynamic` or a `JSON` column
-- have to treat negative zero as equal to positive zero, like `equals` does.

SET allow_suspicious_types_in_group_by = 1;

SELECT 'Dynamic';
SELECT length(groupUniqArray(x)), length(groupArrayDistinct(x)), length(topK(10)(x)), length(groupArrayIntersect([x]))
FROM (SELECT arrayJoin([CAST(0.0, 'Dynamic'), CAST(-0.0, 'Dynamic')]) AS x);
SELECT length(groupUniqArray(x)), length(groupArrayDistinct(x)), length(topK(10)(x))
FROM (SELECT arrayJoin([CAST([0.0, 1.0], 'Dynamic'), CAST([-0.0, 1.0], 'Dynamic')]) AS x);

SELECT 'shared variant of Dynamic';
SELECT length(groupUniqArray(x)), length(groupArrayDistinct(x)), length(topK(10)(x)), length(groupArrayIntersect([x]))
FROM (SELECT arrayJoin([CAST(0.0, 'Dynamic(max_types=0)'), CAST(-0.0, 'Dynamic(max_types=0)')]) AS x);

SELECT 'dynamic path of JSON';
SELECT length(groupUniqArray(x)), length(groupArrayDistinct(x)), length(topK(10)(x))
FROM (SELECT arrayJoin(['{"a":0.0}'::JSON, '{"a":-0.0}'::JSON]) AS x);

SELECT 'shared data of JSON';
SELECT length(groupUniqArray(x)), length(groupArrayDistinct(x)), length(topK(10)(x))
FROM (SELECT arrayJoin(['{"a":0.0}'::JSON(max_dynamic_paths=0), '{"a":-0.0}'::JSON(max_dynamic_paths=0)]) AS x);

SELECT 'typed path of JSON';
SELECT length(groupUniqArray(x)), length(groupArrayDistinct(x)), length(topK(10)(x))
FROM (SELECT arrayJoin(['{"a":0.0}'::JSON(a Float64), '{"a":-0.0}'::JSON(a Float64)]) AS x);

SELECT 'different values are still distinct';
SELECT length(groupUniqArray(x)) FROM (SELECT arrayJoin([CAST(0.0, 'Dynamic'), CAST(-0.0, 'Dynamic'), CAST(1.0, 'Dynamic'), CAST('0', 'Dynamic')]) AS x);
SELECT length(groupUniqArray(x)) FROM (SELECT arrayJoin(['{"a":0.0}'::JSON(max_dynamic_paths=0), '{"a":-0.0}'::JSON(max_dynamic_paths=0), '{"a":1.0}'::JSON(max_dynamic_paths=0), '{"b":0.0}'::JSON(max_dynamic_paths=0)]) AS x);
