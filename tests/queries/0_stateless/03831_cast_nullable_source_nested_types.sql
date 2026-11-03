-- Test that CAST with nullable_source propagation through nested types
-- (Array, Map, Tuple) does not cause "ColumnNullable is not compatible with original".
-- The bug: prepareRemoveNullable sets nullable_source at the Nullable(Tuple) level (N rows),
-- createTupleWrapper passes it to element wrappers, and createArrayWrapper (before the fix)
-- passed it to createStringToEnumWrapper where the inner String column has M total array
-- elements (M != N), causing a size mismatch assertion.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=96894&sha=20e3ac9e9d7a4790a81b166af49e932202510e1e&name_0=PR&name_1=BuzzHouse%20%28amd_debug%29

SET allow_experimental_nullable_tuple_type = 1;

-- Nullable(Tuple(Array(String))) -> Nullable(Tuple(Array(Enum8)))
-- The Nullable at the Tuple level sets nullable_source which must not propagate into Array elements.
SELECT CAST(if(number % 2 = 0, tuple(['a', 'b']), NULL), 'Nullable(Tuple(Array(Enum8(\'a\' = 1, \'b\' = 2))))') FROM numbers(10) FORMAT Null;

-- Same with Map containing Enum values inside a Nullable Tuple
SELECT CAST(if(number % 3 = 0, tuple(map('x', 'a', 'y', 'b')), NULL), 'Nullable(Tuple(Map(String, Enum8(\'a\' = 1, \'b\' = 2))))') FROM numbers(10) FORMAT Null;

SELECT 'OK';
