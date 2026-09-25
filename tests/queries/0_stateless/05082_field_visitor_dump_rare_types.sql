-- FieldVisitorDump converts Field values to human-readable debug strings used by EXPLAIN QUERY TREE,
-- Field::dump(), and exception messages. The analyzer constant-folds typed literals into
-- ConstantNodes whose values are rendered via FieldVisitorDump. Six field types --
-- Decimal256, UInt128, Int128, Int256, IPv4, IPv6 -- and multi-element Map literals
-- had zero CI coverage because no existing test produced query trees containing those
-- Field types. src/Common/FieldVisitorDump.cpp lines 35-36, 38-39, 41-42, 95-96.

-- EXPLAIN QUERY TREE requires the analyzer.
SET enable_analyzer = 1;

-- 1. Decimal256 Field: CAST to Decimal256 constant-folds to a Decimal256 Field.
--    FieldVisitorDump.cpp:35 formats it as "Decimal256_'<value>'".
SELECT countIf(explain LIKE '%Decimal256_%') > 0 AS found
FROM (EXPLAIN QUERY TREE SELECT 1::Decimal256(0));

-- 2. UInt128 Field: CAST to UInt128 constant-folds to a UInt128 Field.
--    FieldVisitorDump.cpp:36 formats it as "UInt128_<value>".
SELECT countIf(explain LIKE '%UInt128_%') > 0 AS found
FROM (EXPLAIN QUERY TREE SELECT 1::UInt128);

-- 3. Int128 Field: similar to UInt128.
--    FieldVisitorDump.cpp:38 formats it as "Int128_<value>".
SELECT countIf(explain LIKE '%Int128_%') > 0 AS found
FROM (EXPLAIN QUERY TREE SELECT 1::Int128);

-- 4. Int256 Field.
--    FieldVisitorDump.cpp:39 formats it as "Int256_<value>".
SELECT countIf(explain LIKE '%Int256_%') > 0 AS found
FROM (EXPLAIN QUERY TREE SELECT 1::Int256);

-- 5. IPv4 Field: toIPv4() is constant-folded by the optimizer.
--    FieldVisitorDump.cpp:41 formats it as "IPv4_'<addr>'".
SELECT countIf(explain LIKE '%IPv4_%') > 0 AS found
FROM (EXPLAIN QUERY TREE SELECT toIPv4('1.2.3.4'));

-- 6. IPv6 Field: toIPv6() is constant-folded by the optimizer.
--    FieldVisitorDump.cpp:42 formats it as "IPv6_'<addr>'".
SELECT countIf(explain LIKE '%IPv6_%') > 0 AS found
FROM (EXPLAIN QUERY TREE SELECT toIPv6('::1'));

-- 7. Map with multiple entries: exercises the comma-separator branch in Map dump.
--    FieldVisitorDump.cpp:95-96: the second and later entries prepend ", ".
--    The output is "Map_(Tuple_(...), Tuple_(...))" with a comma between entries.
SELECT countIf(explain LIKE '%Map_%,%') > 0 AS found
FROM (EXPLAIN QUERY TREE SELECT map(1, 'a', 2, 'b'));
