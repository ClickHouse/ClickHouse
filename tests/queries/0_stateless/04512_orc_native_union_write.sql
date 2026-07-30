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

-- A variant whose first (sorted) branch is String, with many NULL rows. NULL union rows must not
-- make the ORC library consume extra (unfilled) rows from the first branch's batch: it used to
-- dereference uninitialized string pointers there, crashing the writer, or to silently write
-- uninitialized values into the file for numeric branches.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_nulls.orc', ORC, 'v Variant(String, UInt64)')
SELECT multiIf(
        number % 3 = 0, NULL,
        number % 3 = 1, CAST('s' || toString(number), 'Variant(String, UInt64)'),
        CAST(number, 'Variant(String, UInt64)')) AS v
FROM numbers(1000);
SELECT
    count(),
    countIf(v IS NULL),
    countIf(variantType(v) = 'String'),
    countIf(variantType(v) = 'Int64'),
    sum(variantElement(v, 'Int64')),
    min(variantElement(v, 'String')),
    max(variantElement(v, 'String'))
FROM file(currentDatabase() || '_04512_nulls.orc', ORC);

-- Distinct Variant branches that map to the same ORC type would produce an ORC union with
-- duplicate branch types, which the reader rejects, so the writer must reject them up front.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Int32, UInt32)')
SELECT 42::Int32::Variant(Int32, UInt32); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(IPv4, Int32)')
SELECT 42::Int32::Variant(IPv4, Int32); -- { serverError ILLEGAL_COLUMN }
-- A LowCardinality branch is rejected earlier, by the generic unsupported-type check (the writer
-- only strips LowCardinality from top-level columns), so it cannot collide with a plain branch.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(LowCardinality(String), String)')
SELECT CAST(NULL, 'Variant(LowCardinality(String), String)'); -- { serverError ILLEGAL_COLUMN }
