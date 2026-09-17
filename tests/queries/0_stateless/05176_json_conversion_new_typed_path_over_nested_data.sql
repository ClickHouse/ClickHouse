-- Optimized JSON-to-JSON conversion: when the destination adds a typed path and declares nothing
-- below it, the JSON parser treats that path as a scalar slot and raises on a row holding an object
-- there. The optimized path must raise too instead of reusing the paths below it and defaulting the
-- typed path, which would invent a value the row never had.

-- Source data below the new typed path: typed, dynamic and shared data all raise.
SELECT '{"a":{"b":1}}'::JSON(`a.b` UInt32)::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT '{"a":{"b":1}}'::JSON(`a.b` UInt32)::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

SELECT '{"a":{"b":1}}'::JSON(max_dynamic_paths=0)::JSON(a Int32, max_dynamic_paths=0) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT '{"a":{"b":1}}'::JSON(max_dynamic_paths=0)::JSON(a Int32, max_dynamic_paths=0) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

-- Depth does not matter, on either side.
SELECT '{"a":{"b":{"c":1}}}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT '{"a":{"b":{"c":1}}}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

SELECT '{"a":{"b":{"c":1}}}'::JSON::JSON(`a.b` Int32) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT '{"a":{"b":{"c":1}}}'::JSON::JSON(`a.b` Int32) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

-- Both rows in one block: the object row is rejected even though the other row has a scalar.
SELECT arrayJoin(['{"a":42}', '{"a":{"b":1}}'])::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT arrayJoin(['{"a":42}', '{"a":{"b":1}}'])::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

-- A SKIP rule below the leaf changes nothing: the parser fails at the leaf before it would descend
-- and apply the rule. The leaf itself cannot be skipped -- such a type is rejected at parse time.
SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32, SKIP `a.b`) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32, SKIP `a.b`) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

SELECT '{"a":{"b":1}}'::JSON(`a.b` UInt32)::JSON(a Int32, SKIP `a.b`) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT '{"a":{"b":1}}'::JSON(`a.b` UInt32)::JSON(a Int32, SKIP `a.b`) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32, SKIP REGEXP '^a\\.') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32, SKIP REGEXP '^a\\.') SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }

SELECT '--- valid: scalar present in the same row ---';
SELECT '{"a":42,"a":{"b":43}}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"a":42,"a":{"b":43}}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '--- valid: destination declares a typed path below, so the parser descends ---';
SELECT '{"a":{"b":1}}'::JSON(`a.b` UInt32)::JSON(a Int32, `a.b` UInt32) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"a":{"b":1}}'::JSON(`a.b` UInt32)::JSON(a Int32, `a.b` UInt32) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32, `a.b` UInt32) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32, `a.b` UInt32) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '{"a":{"x":1}}'::JSON::JSON(a Int32, `a.b` UInt32) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"a":{"x":1}}'::JSON::JSON(a Int32, `a.b` UInt32) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '{"a":{"b":{"c":1}}}'::JSON::JSON(a Int32, `a.b.c` UInt32) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"a":{"b":{"c":1}}}'::JSON::JSON(a Int32, `a.b.c` UInt32) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '--- valid: sibling path that only shares a prefix ---';
SELECT '{"ab":1}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"ab":1}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1;

SELECT '--- valid: the new typed path is a scalar in the source ---';
SELECT '{"a":1}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0;
SELECT '{"a":1}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1;

-- An empty block reads no value, so it must not raise. `materialize` keeps the cast out of
-- constant folding, which would otherwise evaluate it on one row regardless of `numbers(0)`.
SELECT '--- valid: no rows, so nothing is read and nothing raises ---';
SELECT materialize('{"a":{"b":1}}')::JSON::JSON(a Int32) FROM numbers(0) SETTINGS json_use_optimized_type_conversion = 0;
SELECT materialize('{"a":{"b":1}}')::JSON::JSON(a Int32) FROM numbers(0) SETTINGS json_use_optimized_type_conversion = 1;


-- With type_json_skip_invalid_typed_paths the parser defaults the typed path and drops the data
-- below it. The optimized path cannot reproduce that, so it hands the block to format+parse and
-- both paths must still agree, per row and for the whole subtree.
SELECT '--- type_json_skip_invalid_typed_paths drops the data below the typed path ---';
SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0, type_json_skip_invalid_typed_paths = 1;
SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1, type_json_skip_invalid_typed_paths = 1;

SELECT '{"a":{"b":{"c":1},"d":2},"z":9}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0, type_json_skip_invalid_typed_paths = 1;
SELECT '{"a":{"b":{"c":1},"d":2},"z":9}'::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1, type_json_skip_invalid_typed_paths = 1;

SELECT materialize(arrayJoin(['{"a":42,"a":{"b":43}}','{"a":{"b":1}}']))::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 0, type_json_skip_invalid_typed_paths = 1;
SELECT materialize(arrayJoin(['{"a":42,"a":{"b":43}}','{"a":{"b":1}}']))::JSON::JSON(a Int32) SETTINGS json_use_optimized_type_conversion = 1, type_json_skip_invalid_typed_paths = 1;

SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32, SKIP `a.b`) SETTINGS json_use_optimized_type_conversion = 0, type_json_skip_invalid_typed_paths = 1;
SELECT '{"a":{"b":1}}'::JSON::JSON(a Int32, SKIP `a.b`) SETTINGS json_use_optimized_type_conversion = 1, type_json_skip_invalid_typed_paths = 1;
