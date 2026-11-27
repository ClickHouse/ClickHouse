-- Comparison of arrays of Nullable elements whose element types have no least common
-- supertype (mixed signed/unsigned integers wider than 32 bits). Inner NULLs follow array
-- semantics: a NULL is a regular comparable value that is equal only to another NULL and
-- sorts after every non-NULL value.

-- all six operators, non-NULL values'
SELECT [-1]::Array(Nullable(Int64)) =  [1]::Array(Nullable(UInt64));
SELECT [-1]::Array(Nullable(Int64)) != [1]::Array(Nullable(UInt64));
SELECT [-1]::Array(Nullable(Int64)) <  [1]::Array(Nullable(UInt64));
SELECT [-1]::Array(Nullable(Int64)) <= [1]::Array(Nullable(UInt64));
SELECT [-1]::Array(Nullable(Int64)) >  [1]::Array(Nullable(UInt64));
SELECT [-1]::Array(Nullable(Int64)) >= [1]::Array(Nullable(UInt64));

-- NULL equals only NULL
SELECT [NULL]::Array(Nullable(Int64)) =  [NULL]::Array(Nullable(UInt64));
SELECT [NULL]::Array(Nullable(Int64)) != [NULL]::Array(Nullable(UInt64));
SELECT [NULL]::Array(Nullable(Int64)) =  [1]::Array(Nullable(UInt64));
SELECT [NULL]::Array(Nullable(Int64)) != [1]::Array(Nullable(UInt64));

-- NULL sorts after every non-NULL value
SELECT [NULL]::Array(Nullable(Int64)) <  [1]::Array(Nullable(UInt64));
SELECT [1]::Array(Nullable(Int64))    <  [NULL]::Array(Nullable(UInt64));
SELECT [NULL]::Array(Nullable(Int64)) >  [1]::Array(Nullable(UInt64));
SELECT [1]::Array(Nullable(Int64))    >  [NULL]::Array(Nullable(UInt64));
SELECT [NULL]::Array(Nullable(Int64)) >= [NULL]::Array(Nullable(UInt64));
SELECT [NULL]::Array(Nullable(Int64)) <= [NULL]::Array(Nullable(UInt64));

-- NULL inside the common prefix
SELECT [1, NULL, 3]::Array(Nullable(Int64)) =  [1, NULL, 3]::Array(Nullable(UInt64));
SELECT [1, NULL, 3]::Array(Nullable(Int64)) =  [1, NULL, 4]::Array(Nullable(UInt64));
SELECT [1, NULL]::Array(Nullable(Int64))    <  [1, 2]::Array(Nullable(UInt64));
SELECT [1, 2]::Array(Nullable(Int64))       <  [1, NULL]::Array(Nullable(UInt64));

-- length tie-break on equal common prefix (with NULLs)
SELECT [1, NULL]::Array(Nullable(Int64))    <  [1, NULL, 3]::Array(Nullable(UInt64));
SELECT [1, NULL, 3]::Array(Nullable(Int64)) <  [1, NULL]::Array(Nullable(UInt64));

-- mixed nullability: one side Nullable elements, the other not
SELECT [1]::Array(Nullable(UInt64)) =  [1]::Array(Int64);
SELECT [1]::Array(UInt64)           =  [1]::Array(Nullable(Int64));
SELECT [-1]::Array(Int64)           <  [1]::Array(Nullable(UInt64));

-- non-constant (materialized) path
SELECT materialize([NULL]::Array(Nullable(Int64))) <  materialize([1]::Array(Nullable(UInt64)));
SELECT materialize([1]::Array(Nullable(Int64)))    <  materialize([NULL]::Array(Nullable(UInt64)));
SELECT materialize([NULL]::Array(Nullable(Int64))) =  materialize([NULL]::Array(Nullable(UInt64)));
SELECT materialize([NULL]::Array(Nullable(Int64))) != materialize([1]::Array(Nullable(UInt64)));

-- wide integers
SELECT [-1]::Array(Nullable(Int256))   <  [1]::Array(Nullable(UInt256));
SELECT [NULL]::Array(Nullable(Int256)) =  [NULL]::Array(Nullable(UInt256));

-- nested arrays with Nullable innermost elements
SELECT [[1, NULL]]::Array(Array(Nullable(Int64))) = [[1, NULL]]::Array(Array(Nullable(UInt64)));

-- multi-row via table (per-row NULLs and offsets)
DROP TABLE IF EXISTS t_arr_cmp_null;
CREATE TABLE t_arr_cmp_null (a Array(Nullable(Int64)), b Array(Nullable(UInt64))) ENGINE = Memory;
INSERT INTO t_arr_cmp_null VALUES ([1, NULL], [1, 3]), ([NULL], [1]), ([1], [NULL]), ([NULL], [NULL]), ([1, 2], [1, 2]);
SELECT a < b, a = b, a > b FROM t_arr_cmp_null ORDER BY a, b;
DROP TABLE t_arr_cmp_null;

-- result type is definite UInt8 (not Nullable)
SELECT toTypeName([1]::Array(Nullable(Int64)) = [1]::Array(Nullable(UInt64)));
SELECT toTypeName([1]::Array(Nullable(Int64)) < [1]::Array(Nullable(UInt64)));

-- consistency with the least-common-supertype path (same-type Nullable arrays)
SELECT ([NULL]::Array(Nullable(Int64)) =  [NULL]::Array(Nullable(Int64))) =  ([NULL]::Array(Nullable(Int64)) =  [NULL]::Array(Nullable(UInt64)));
SELECT ([1]::Array(Nullable(Int64))    <  [NULL]::Array(Nullable(Int64))) =  ([1]::Array(Nullable(Int64))    <  [NULL]::Array(Nullable(UInt64)));

-- incomparable element types still throw
SELECT ['a']::Array(Nullable(String)) < [1]::Array(Nullable(Int64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Array of Tuple with a nested Nullable field: EQUALITY is comparable (array null-as-value
-- semantics let the executor use the null-safe equals element comparator), and matches the
-- equivalent bare-tuple / null-safe result. ORDERING has no null-safe primitive for nested NULLs,
-- so it still throws.
SELECT [tuple(CAST(1, 'Nullable(UInt64)'))]::Array(Tuple(Nullable(UInt64))) =  [tuple(CAST(1, 'Nullable(Int64)'))]::Array(Tuple(Nullable(Int64)));
SELECT [tuple(CAST(1, 'Nullable(UInt64)'))]::Array(Tuple(Nullable(UInt64))) != [tuple(CAST(2, 'Nullable(Int64)'))]::Array(Tuple(Nullable(Int64)));
SELECT [tuple(CAST(NULL, 'Nullable(UInt64)'))]::Array(Tuple(Nullable(UInt64))) = [tuple(CAST(NULL, 'Nullable(Int64)'))]::Array(Tuple(Nullable(Int64)));
SELECT [tuple(CAST(NULL, 'Nullable(UInt64)'))]::Array(Tuple(Nullable(UInt64))) = [tuple(CAST(1, 'Nullable(Int64)'))]::Array(Tuple(Nullable(Int64)));
-- matches the null-safe (IS NOT DISTINCT FROM) element semantics on the same values
SELECT ([tuple(CAST(1,'Nullable(UInt64)'))]::Array(Tuple(Nullable(UInt64))) = [tuple(CAST(1,'Nullable(Int64)'))]::Array(Tuple(Nullable(Int64)))) = ((CAST(1,'Nullable(UInt64)'),) IS NOT DISTINCT FROM (CAST(1,'Nullable(Int64)'),));
SELECT [tuple(CAST(1, 'Nullable(UInt64)'))]::Array(Tuple(Nullable(UInt64))) < [tuple(CAST(1, 'Nullable(Int64)'))]::Array(Tuple(Nullable(Int64))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- a string vs non-string mismatch nested inside a composite element type is rejected during analysis, not at execution
SELECT [tuple('1')]::Array(Tuple(String)) = [tuple(1)]::Array(Tuple(Int64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [tuple(tuple('1'))]::Array(Tuple(Tuple(String))) = [tuple(tuple(1))]::Array(Tuple(Tuple(Int64))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- an aligned String-vs-String subfield with a nested Nullable field in another subfield: EQUALITY is
-- now comparable (aligned strings are fine, and equality tolerates the Nullable element result);
-- ORDERING still throws.
SELECT [tuple('a', CAST(1, 'Nullable(UInt64)'))]::Array(Tuple(String, Nullable(UInt64))) = [tuple('a', CAST(1, 'Nullable(Int64)'))]::Array(Tuple(String, Nullable(Int64)));
SELECT [tuple('a', CAST(1, 'Nullable(UInt64)'))]::Array(Tuple(String, Nullable(UInt64))) < [tuple('a', CAST(1, 'Nullable(Int64)'))]::Array(Tuple(String, Nullable(Int64))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An element pair with no least supertype where one side is `Nullable(Nothing)` (an untyped NULL
-- or an empty array cast to it) is decided by that side's null map, so it answers instead of
-- aborting. The values match the supertype path (`[NULL] = [1]` etc.).
SELECT [NULL] = [[1]], [NULL] != [[1]], [NULL] < [[1]], [NULL] <= [[1]], [NULL] > [[1]], [NULL] >= [[1]];
SELECT [[1]] = [NULL], [[1]] > [NULL];
SELECT isNotDistinctFrom([NULL], [[1]]), isDistinctFrom([NULL], [[1]]);
SELECT [NULL] = [map(toUInt8(1), toUInt8(1))];
-- the same with an empty aligned prefix, where the length tie-break alone decides
SELECT CAST([], 'Array(Nullable(Nothing))') = [[1]], CAST([], 'Array(Nullable(Nothing))') != [[1]],
       CAST([], 'Array(Nullable(Nothing))') < [[1]], CAST([], 'Array(Nullable(Nothing))') <= [[1]],
       CAST([], 'Array(Nullable(Nothing))') > [[1]], CAST([], 'Array(Nullable(Nothing))') >= [[1]];
SELECT isNotDistinctFrom(CAST([], 'Array(Nullable(Nothing))'), [[1]]), isDistinctFrom(CAST([], 'Array(Nullable(Nothing))'), [[1]]);

-- `Array(Nothing)` and `Nullable(Nothing)` inside a tuple element share a supertype with their
-- partner, so those positions never reach the element path and keep answering.
SELECT [tuple(CAST([], 'Array(Nothing)'), toUInt64(1))] =  [tuple([1], toInt64(-1))];
SELECT [tuple(CAST([], 'Array(Nothing)'), toUInt64(1))] != [tuple([1], toInt64(-1))];
SELECT [tuple(NULL, toUInt64(1))] = [tuple([1], toInt64(-1))];
SELECT [CAST([], 'Array(Nothing)')] = [[1]], [CAST([], 'Array(Nullable(Nothing))')] = [[1]];
-- an accepted pair declares a plain `UInt8`, matching what the comparison actually produces
SELECT toTypeName([NULL] = [[1]]), toTypeName(CAST([], 'Array(Nullable(Nothing))') > [[1]]);
