-- Regardless of the setting allow_experimental_nullable_tuple_type, the output should be same.
-- The behavior is controlled by `allow_nullable_tuple_in_extracted_subcolumns` from global context.

-- { echo }

SET enable_analyzer = 1;

SET allow_experimental_nullable_tuple_type = 0;

SELECT toTypeName(v.`Tuple(UInt64, String)`), v.`Tuple(UInt64, String)` FROM (SELECT 42::Variant(Tuple(UInt64, String), UInt64) AS v);
SELECT toTypeName(d.`Tuple(UInt64, String)`), d.`Tuple(UInt64, String)` FROM (SELECT 42::Dynamic AS d);
SELECT toTypeName(j.c.:`Tuple(UInt64, String)`), j.c.:`Tuple(UInt64, String)` FROM (SELECT CAST('{"a":1}', 'JSON(a UInt64)') AS j);
SELECT toTypeName(d.`Tuple(UInt64, String)`), d.`Tuple(UInt64, String)` FROM (SELECT (1, 'x')::Tuple(UInt64, String)::Dynamic AS d);

SET allow_experimental_nullable_tuple_type = 1;

SELECT toTypeName(v.`Tuple(UInt64, String)`), v.`Tuple(UInt64, String)` FROM (SELECT 42::Variant(Tuple(UInt64, String), UInt64) AS v);
SELECT toTypeName(d.`Tuple(UInt64, String)`), d.`Tuple(UInt64, String)` FROM (SELECT 42::Dynamic AS d);
SELECT toTypeName(j.c.:`Tuple(UInt64, String)`), j.c.:`Tuple(UInt64, String)` FROM (SELECT CAST('{"a":1}', 'JSON(a UInt64)') AS j);
SELECT toTypeName(d.`Tuple(UInt64, String)`), d.`Tuple(UInt64, String)` FROM (SELECT (1, 'x')::Tuple(UInt64, String)::Dynamic AS d);

DROP TABLE IF EXISTS test_variant;
CREATE TABLE test_variant (v Variant(Tuple(UInt64, String), UInt64)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test_variant VALUES (CAST(tuple(toUInt64(1), 'x'), 'Variant(Tuple(UInt64, String), UInt64)'));
INSERT INTO test_variant VALUES (CAST(toUInt64(5), 'Variant(Tuple(UInt64, String), UInt64)'));

SET allow_experimental_nullable_tuple_type = 0;
SELECT toTypeName(getSubcolumn(v, 'Tuple(UInt64, String)')), getSubcolumn(v, 'Tuple(UInt64, String)') FROM test_variant ORDER BY getSubcolumn(v, 'Tuple(UInt64, String)');
SELECT toTypeName(getSubcolumn(v, 'Tuple(UInt64, String).null')), getSubcolumn(v, 'Tuple(UInt64, String).null') FROM test_variant ORDER BY getSubcolumn(v, 'Tuple(UInt64, String).null'); -- { serverError ILLEGAL_COLUMN }
SELECT toTypeName(getSubcolumn(v, 'UInt64')), getSubcolumn(v, 'UInt64') FROM test_variant ORDER BY isNull(getSubcolumn(v, 'UInt64')), getSubcolumn(v, 'UInt64');

SET allow_experimental_nullable_tuple_type = 1;
SELECT toTypeName(getSubcolumn(v, 'Tuple(UInt64, String)')), getSubcolumn(v, 'Tuple(UInt64, String)') FROM test_variant ORDER BY isNull(getSubcolumn(v, 'Tuple(UInt64, String)')), getSubcolumn(v, 'Tuple(UInt64, String)');
SELECT toTypeName(getSubcolumn(v, 'Tuple(UInt64, String).null')), getSubcolumn(v, 'Tuple(UInt64, String).null') FROM test_variant ORDER BY getSubcolumn(v, 'Tuple(UInt64, String).null'); -- { serverError ILLEGAL_COLUMN }
SELECT toTypeName(getSubcolumn(v, 'UInt64')), getSubcolumn(v, 'UInt64') FROM test_variant ORDER BY isNull(getSubcolumn(v, 'UInt64')), getSubcolumn(v, 'UInt64');
DROP TABLE test_variant;

DROP TABLE IF EXISTS test_dynamic;
CREATE TABLE test_dynamic (d Dynamic(max_types=1)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test_dynamic VALUES (CAST(toUInt64(10), 'Dynamic(max_types=1)'));
INSERT INTO test_dynamic VALUES (CAST(tuple(toUInt64(1), 'x'), 'Dynamic(max_types=1)'));

SET allow_experimental_nullable_tuple_type = 0;
SELECT toTypeName(getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String))')), getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String))') FROM test_dynamic ORDER BY getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String))');
SELECT toTypeName(getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null')), getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null') FROM test_dynamic ORDER BY getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null'); -- { serverError ILLEGAL_COLUMN }
SELECT toTypeName(getSubcolumn(d, 'UInt64')), getSubcolumn(d, 'UInt64') FROM test_dynamic ORDER BY isNull(getSubcolumn(d, 'UInt64')), getSubcolumn(d, 'UInt64');

SET allow_experimental_nullable_tuple_type = 1;
SELECT toTypeName(getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String))')), getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String))') FROM test_dynamic ORDER BY isNull(getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String))')), getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String))');
SELECT toTypeName(getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null')), getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null') FROM test_dynamic ORDER BY getSubcolumn(d, 'Tuple(Nullable(UInt64), Nullable(String)).null'); -- { serverError ILLEGAL_COLUMN }
SELECT toTypeName(getSubcolumn(d, 'UInt64')), getSubcolumn(d, 'UInt64') FROM test_dynamic ORDER BY isNull(getSubcolumn(d, 'UInt64')), getSubcolumn(d, 'UInt64');
DROP TABLE test_dynamic;

SET allow_experimental_nullable_tuple_type = 0;
SELECT toTypeName(getSubcolumn(v, 'LowCardinality(String)')), getSubcolumn(v, 'LowCardinality(String)') FROM (SELECT CAST('x', 'LowCardinality(String)')::Variant(LowCardinality(String), UInt64) AS v);
SELECT toTypeName(getSubcolumn(d, 'LowCardinality(String)')), getSubcolumn(d, 'LowCardinality(String)') FROM (SELECT CAST('x', 'LowCardinality(String)')::Dynamic AS d);

SET allow_experimental_nullable_tuple_type = 1;
SELECT toTypeName(getSubcolumn(v, 'LowCardinality(String)')), getSubcolumn(v, 'LowCardinality(String)') FROM (SELECT CAST('x', 'LowCardinality(String)')::Variant(LowCardinality(String), UInt64) AS v);
SELECT toTypeName(getSubcolumn(d, 'LowCardinality(String)')), getSubcolumn(d, 'LowCardinality(String)') FROM (SELECT CAST('x', 'LowCardinality(String)')::Dynamic AS d);
