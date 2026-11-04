-- These tuple .null subcolumns should succeed when
-- allow_nullable_tuple_in_extracted_subcolumns = 1.

SET enable_analyzer = 1;

SET allow_experimental_nullable_tuple_type = 0;
SELECT toTypeName(getSubcolumn(v, 'Tuple(UInt64, String).null')), getSubcolumn(v, 'Tuple(UInt64, String).null') FROM (SELECT 42::Variant(Tuple(UInt64, String), UInt64) AS v);
SELECT toTypeName(getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null')), getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null') FROM (SELECT 42::Dynamic AS d);

SET allow_experimental_nullable_tuple_type = 1;
SELECT toTypeName(getSubcolumn(v, 'Tuple(UInt64, String).null')), getSubcolumn(v, 'Tuple(UInt64, String).null') FROM (SELECT 42::Variant(Tuple(UInt64, String), UInt64) AS v);
SELECT toTypeName(getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null')), getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null') FROM (SELECT 42::Dynamic AS d);
