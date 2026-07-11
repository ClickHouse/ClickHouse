-- Tags: no-fasttest
-- no-fasttest: requires the ORC input/output format, which is not built in fasttest.

-- The native ORC writer maps the ClickHouse Variant type to an ORC uniontype, and the native reader
-- maps it back, so a Variant survives an ORC round-trip (including NULL rows and branch reordering).

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET engine_file_truncate_on_insert = 1;

-- Two branches, including a NULL row.
INSERT INTO FUNCTION file(currentDatabase() || '_04512.orc', ORC, 'v Variant(Int32, String)')
      SELECT 42::Int32::Variant(Int32, String)
UNION ALL SELECT 'hello'::String::Variant(Int32, String)
UNION ALL SELECT CAST(NULL, 'Variant(Int32, String)')
UNION ALL SELECT 7::Int32::Variant(Int32, String);
SELECT v, toTypeName(v) FROM file(currentDatabase() || '_04512.orc', ORC) ORDER BY toString(v);

-- Schema inference of the written file yields the Variant type again.
DESC file(currentDatabase() || '_04512.orc', ORC);

-- Three branches: Variant sorts the branch types, so the tags/discriminators are remapped on write and read.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_3.orc', ORC, 'v Variant(Int32, String, Float64)')
      SELECT 42::Int32::Variant(Int32, String, Float64)
UNION ALL SELECT 'hi'::String::Variant(Int32, String, Float64)
UNION ALL SELECT (3.14::Float64)::Variant(Int32, String, Float64)
UNION ALL SELECT CAST(NULL, 'Variant(Int32, String, Float64)');
SELECT v, toTypeName(v) FROM file(currentDatabase() || '_04512_3.orc', ORC) ORDER BY toString(v);
