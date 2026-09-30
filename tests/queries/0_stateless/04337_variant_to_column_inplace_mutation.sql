-- Regression test: converting a single-variant-plus-NULLs ColumnVariant with a function that
-- returns its input column unchanged (toString of a String variant, concat of one String argument)
-- used to mutate the shared variant subcolumn in place via assumeMutable()->expand(), leaving the
-- source ColumnVariant malformed (a sub-column larger than the discriminators pointing at it).
-- The next operation that validates the variant (permute via arraySort, native serialization via
-- blockSerializedSize) then aborted with "Variant N has size X, but expected Y".
-- https://github.com/ClickHouse/ClickHouse/issues/101415

SET enable_variant_type = 1;
SET allow_suspicious_variant_types = 1;

-- Conversion path (ConvertImplFromVariantToColumn): toString returns the String subcolumn unchanged.
SELECT 'conversion path, const array';
SELECT arraySort(x -> toString(x), [NULL, CAST('hi', 'Variant(Int32, String)')]);

-- Adaptor path (FunctionVariantAdaptor): concat of a single String argument returns it unchanged.
SELECT 'adaptor path, const array';
SELECT arraySort(x -> concat(toString(x)), [NULL, CAST('hi', 'Variant(Int32, String)')]);

-- Table-backed columns: the source variant must stay intact after the function, so a following
-- blockSerializedSize over the same column does not see a malformed variant.
DROP TABLE IF EXISTS t04337;
CREATE TABLE t04337 (id UInt32, v Variant(Int32, String)) ENGINE = Memory;
INSERT INTO t04337 VALUES (1, NULL), (2, 'hi');

SELECT 'conversion path, table column';
SELECT toString(v), blockSerializedSize(v) FROM t04337 ORDER BY id;

SELECT 'adaptor path, table column';
SELECT concat(v), blockSerializedSize(v) FROM t04337 ORDER BY id;

DROP TABLE t04337;

-- Same bug in the sibling Dynamic adaptor (FunctionDynamicAdaptor): Dynamic is backed by a
-- ColumnVariant, and the single-type-plus-NULL fast path also expanded the (possibly shared)
-- nested function result in place via assumeMutable()->expand().
SET enable_dynamic_type = 1;

SELECT 'dynamic adaptor path, const array';
SELECT arraySort(x -> concat(toString(x)), [NULL, CAST('hi', 'Dynamic')]);

DROP TABLE IF EXISTS t04337_dyn;
CREATE TABLE t04337_dyn (id UInt32, d Dynamic) ENGINE = Memory;
INSERT INTO t04337_dyn VALUES (1, NULL), (2, 'hi');

-- concat of a single Dynamic argument returns the String subcolumn unchanged; blockSerializedSize
-- then re-validates the source Dynamic, which used to abort with "Variant N has size X, expected Y".
SELECT 'dynamic adaptor path, table column';
SELECT concat(d), blockSerializedSize(d) FROM t04337_dyn ORDER BY id;

SELECT 'dynamic toString, table column';
SELECT toString(d), blockSerializedSize(d) FROM t04337_dyn ORDER BY id;

DROP TABLE t04337_dyn;
