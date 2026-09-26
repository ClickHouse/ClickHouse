SELECT '-- Basic numeric arrays:';
SELECT arrayTopK(3, [1, 5, 2, 7, 3]);
SELECT arrayBottomK(3, [1, 5, 2, 7, 3]);
SELECT arrayTopK(0, [1, 5, 2, 7, 3]);
SELECT arrayBottomK(0, [1, 5, 2, 7, 3]);

SELECT '-- K larger than array size:';
SELECT arrayTopK(10, [1, 5, 2, 7, 3]);
SELECT arrayBottomK(10, [1, 5, 2, 7, 3]);

SELECT '-- Empty input:';
SELECT arrayTopK(5, emptyArrayUInt32());
SELECT arrayBottomK(5, emptyArrayUInt32());

SELECT '-- Nulls are skipped, result element type is non-nullable:';
SELECT arrayTopK(3, [1, NULL, 5, 2, NULL, 7]);
SELECT arrayBottomK(3, [1, NULL, 5, 2, NULL, 7]);
SELECT toTypeName(arrayTopK(3, [1, NULL, 5]));
SELECT toTypeName(arrayBottomK(3, [1, NULL, 5]));

SELECT '-- Result may be shorter than K when non-null count is lower:';
SELECT arrayTopK(5, [1, NULL, 2]);
SELECT arrayBottomK(5, [1, NULL, 2]);

SELECT '-- All-null input returns empty array:';
SELECT arrayTopK(3, [NULL, NULL, NULL]::Array(Nullable(UInt32)));
SELECT arrayBottomK(3, [NULL, NULL, NULL]::Array(Nullable(UInt32)));

SELECT '-- Strings:';
SELECT arrayTopK(2, ['alpha', 'beta', 'gamma', 'delta']);
SELECT arrayBottomK(2, ['alpha', 'beta', 'gamma', 'delta']);
SELECT arrayTopK(2, ['alpha', NULL, 'gamma', NULL]);

SELECT '-- Lambda with single argument:';
SELECT arrayTopK((x) -> -x, 2, [5, 9, 1, 3]);
SELECT arrayBottomK((x) -> -x, 2, [5, 9, 1, 3]);

SELECT '-- Lambda with multiple arguments: result values come from the first array, order from lambda:';
SELECT arrayTopK((x, y) -> y, 2, ['a', 'b', 'c'], [3, 1, 2]);
SELECT arrayBottomK((x, y) -> y, 2, ['a', 'b', 'c'], [3, 1, 2]);

SELECT '-- Lambda that produces NULLs; the corresponding original elements must be skipped:';
SELECT arrayTopK((x) -> if(x % 2 = 0, NULL, x), 3, [1, 2, 3, 4, 5, 6, 7]);
SELECT arrayBottomK((x) -> if(x % 2 = 0, NULL, x), 3, [1, 2, 3, 4, 5, 6, 7]);

SELECT '-- Constant-NULL lambda: every mapped key is NULL, so every element must be skipped:';
SELECT arrayTopK((x) -> NULL, 2, [1, 2, 3]);
SELECT arrayBottomK((x) -> NULL, 2, [1, 2, 3]);

SELECT '-- With materialize to force a non-const column path:';
SELECT arrayTopK(3, materialize([1, NULL, 5, 2, NULL, 7]));
SELECT arrayBottomK(3, materialize([1, NULL, 5, 2, NULL, 7]));

SELECT '-- Per-row varying input:';
SELECT arrayTopK(2, arr), arrayBottomK(2, arr)
FROM (SELECT arrayMap(x -> x, range(number)) AS arr FROM numbers(6));

SELECT '-- K as a materialized column value:';
SELECT arrayTopK(materialize(toUInt32(2)), [1, 5, 2, 7, 3]);

SELECT '-- LowCardinality is removed:';
SELECT arrayTopK(2, ['alpha', 'beta', 'gamma', 'delta']::Array(LowCardinality(String)));
SELECT arrayBottomK(2, ['alpha', 'beta', 'gamma', 'delta']::Array(LowCardinality(String)));
SELECT toTypeName(arrayTopK(2, ['alpha', 'beta', 'gamma']::Array(LowCardinality(String))));
SELECT arrayTopK(3, ['alpha', NULL, 'gamma', NULL, 'beta']::Array(LowCardinality(Nullable(String))));
SELECT arrayBottomK(3, ['alpha', NULL, 'gamma', NULL, 'beta']::Array(LowCardinality(Nullable(String))));
SELECT toTypeName(arrayTopK(3, ['alpha', NULL, 'beta']::Array(LowCardinality(Nullable(String)))));
SELECT toTypeName(arrayBottomK(3, ['alpha', NULL, 'beta']::Array(LowCardinality(Nullable(String)))));

SELECT '-- Errors:';
SELECT arrayTopK([1, 2, 3]);                              -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayBottomK([1, 2, 3]);                           -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayTopK('foo', [1, 2, 3]);                       -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTopK(2, [1, 2, 3], [1]);                      -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTopK(2, [1, 2, 3], 3);                        -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayTopK(-1, [1, 5, 2, 7, 3]);                    -- { serverError BAD_ARGUMENTS }
SELECT arrayBottomK(-1, [1, 5, 2, 7, 3]);                 -- { serverError BAD_ARGUMENTS }
SELECT arrayTopK(materialize(toInt32(-3)), [1, 5, 2]);    -- { serverError BAD_ARGUMENTS }
