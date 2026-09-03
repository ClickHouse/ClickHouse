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

-- Branches whose ORC types differ but which the reader parses back to the same ClickHouse type must
-- be rejected too: ORC `binary` (Int128/UInt128/Int256/UInt256/Decimal256/IPv6) and ORC `string`
-- (String/FixedString) both read as String. Such a file used to be written, and could then not be
-- inferred at all.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(String, Int128)')
SELECT CAST(NULL, 'Variant(String, Int128)'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(String, IPv6)')
SELECT CAST(NULL, 'Variant(String, IPv6)'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(String, Decimal256(5))')
SELECT CAST(NULL, 'Variant(String, Decimal256(5))'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(FixedString(3), Int128)')
SELECT CAST(NULL, 'Variant(FixedString(3), Int128)'); -- { serverError ILLEGAL_COLUMN }

-- The same collapse inside a container: branch types are compared recursively.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Array(String), Array(Int128))')
SELECT CAST(NULL, 'Variant(Array(String), Array(Int128))'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Array(Array(String)), Array(Array(IPv6)))')
SELECT CAST(NULL, 'Variant(Array(Array(String)), Array(Array(IPv6)))'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Array(Nullable(String)), Array(Nullable(Int128)))')
SELECT CAST(NULL, 'Variant(Array(Nullable(String)), Array(Nullable(Int128)))'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Tuple(a String, b Int32), Tuple(a IPv6, b Int32))')
SELECT CAST(NULL, 'Variant(Tuple(a String, b Int32), Tuple(a IPv6, b Int32))'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Map(String, String), Map(String, Int128))')
SELECT CAST(NULL, 'Variant(Map(String, String), Map(String, Int128))'); -- { serverError ILLEGAL_COLUMN }

-- Sibling branches that differ only inside a nested union. A Variant sorts its branches while an ORC
-- union keeps them positional, so these two nested unions are rendered in different orders and are
-- recognized as equivalent only because nested union branches are sorted before comparing.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Array(Variant(String, Int32)), Array(Variant(IPv6, Int32)))')
SELECT CAST(NULL, 'Variant(Array(Variant(String, Int32)), Array(Variant(IPv6, Int32)))'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Tuple(a Variant(String, Int32)), Tuple(a Variant(IPv6, Int32)))')
SELECT CAST(NULL, 'Variant(Tuple(a Variant(String, Int32)), Tuple(a Variant(IPv6, Int32)))'); -- { serverError ILLEGAL_COLUMN }

-- A nested Variant whose own branches collapse is rejected at its own level.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Array(Variant(String, Int128)), Int32)')
SELECT CAST(NULL, 'Variant(Array(Variant(String, Int128)), Int32)'); -- { serverError ILLEGAL_COLUMN }
INSERT INTO FUNCTION file(currentDatabase() || '_04512_dup.orc', ORC, 'v Variant(Tuple(a Variant(String, IPv6)), Int32)')
SELECT CAST(NULL, 'Variant(Tuple(a Variant(String, IPv6)), Int32)'); -- { serverError ILLEGAL_COLUMN }

-- Branches that only look alike must still round-trip: a Tuple field name is significant, and so is
-- decimal precision, so neither pair collapses to one type on read.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_names.orc', ORC, 'v Variant(Tuple(binary Int32), Tuple(string Int32))')
SELECT CAST(NULL, 'Variant(Tuple(binary Int32), Tuple(string Int32))');
DESC file(currentDatabase() || '_04512_names.orc', ORC);

-- A struct field name may itself contain the punctuation that renders a field, including a rendered
-- child key, so field names are length-framed and two distinct tuples cannot render one key.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_punct.orc', ORC, 'v Variant(Tuple(`a:3<>,b` Int32), Tuple(a Int32, b Int32))')
SELECT CAST(NULL, 'Variant(Tuple(`a:3<>,b` Int32), Tuple(a Int32, b Int32))');
DESC file(currentDatabase() || '_04512_punct.orc', ORC);

INSERT INTO FUNCTION file(currentDatabase() || '_04512_dec.orc', ORC, 'v Variant(Decimal64(2), Decimal128(2))')
      SELECT CAST(1.5::Decimal64(2), 'Variant(Decimal64(2), Decimal128(2))')
UNION ALL SELECT CAST(2.5::Decimal128(2), 'Variant(Decimal64(2), Decimal128(2))');
SELECT v, toTypeName(v) FROM file(currentDatabase() || '_04512_dec.orc', ORC) ORDER BY toString(v);

-- Two nested unions that are genuinely distinct: sorting their branches must not merge them.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_nested_ok.orc', ORC, 'v Variant(Array(Variant(String, Int32)), Array(Variant(Float64, Int32)))')
SELECT CAST(NULL, 'Variant(Array(Variant(String, Int32)), Array(Variant(Float64, Int32)))');
DESC file(currentDatabase() || '_04512_nested_ok.orc', ORC);

-- Only the branches of a nested union are order-insensitive. The children of an ORC list, map or
-- struct are positional, because their order is part of the Map or Tuple type, so branches that
-- differ only in child order are different types and must round-trip.
INSERT INTO FUNCTION file(currentDatabase() || '_04512_map_pos.orc', ORC, 'v Variant(Map(Int32, String), Map(String, Int32))')
SELECT CAST(NULL, 'Variant(Map(Int32, String), Map(String, Int32))');
DESC file(currentDatabase() || '_04512_map_pos.orc', ORC);

INSERT INTO FUNCTION file(currentDatabase() || '_04512_tuple_pos.orc', ORC, 'v Variant(Tuple(a Int32, b String), Tuple(b String, a Int32))')
SELECT CAST(NULL, 'Variant(Tuple(a Int32, b String), Tuple(b String, a Int32))');
DESC file(currentDatabase() || '_04512_tuple_pos.orc', ORC);
