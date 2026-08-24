-- A Tuple read from separate CSV columns starts with its first element, so a leading `\N` is the
-- value of that element, not the whole column being NULL. Issue #106247.

SET input_format_csv_deserialize_separate_columns_into_tuple = 1;

-- The reported case: the row was lost before the fix.
SELECT 'first element Nullable';
SELECT * FROM format(CSV, 'x Tuple(Nullable(Int32), Int32)', $$\N,1$$);

-- First element not Nullable: null_as_default puts the element default in it.
SELECT 'first element not Nullable, null_as_default=1';
SELECT * FROM format(CSV, 'x Tuple(Int32, Int32)', $$\N,1$$);

SELECT 'first element not Nullable, null_as_default=0';
SELECT * FROM format(CSV, 'x Tuple(Int32, Int32)', $$\N,1$$) SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT 'first element Nullable, null_as_default=0';
SELECT * FROM format(CSV, 'x Tuple(Nullable(Int32), Int32)', $$\N,1$$) SETTINGS input_format_null_as_default = 0;

-- Every element position, both nullable and not.
SELECT 'nullable combos';
SELECT * FROM format(CSV, 'x Tuple(Nullable(Int32), Nullable(Int32))', $$\N,\N
2,\N
\N,3
4,5$$) ORDER BY x.1, x.2;

SELECT 'not nullable, nulls in both positions';
SELECT * FROM format(CSV, 'x Tuple(Int32, Int32)', $$\N,\N
1,\N
\N,2$$) ORDER BY x.1, x.2;

SELECT 'three elements';
SELECT * FROM format(CSV, 'x Tuple(Nullable(Int32), Nullable(Int32), Int32)', $$\N,\N,3$$);

-- A nested Tuple is flattened too, so the cells after the leading one belong to its elements. The
-- leading `\N` here is the outer tuple's own first element; a null inside a nested tuple is read as
-- that whole nested element and is not covered.
SELECT 'leading null with a nested tuple after it';
SELECT * FROM format(CSV, 'x Tuple(Nullable(Int32), Tuple(Int32, Int32))', $$\N,2,3$$);

-- A Tuple occupies one cell per element, so a following column is read from the cell after the
-- last element.
SELECT 'tuple followed by a column';
SELECT * FROM format(CSV, 'a UInt8, t Tuple(Nullable(Int32), Int32), b UInt8', $$1,\N,5,2$$);

SELECT 'two tuples';
SELECT * FROM format(CSV, 't1 Tuple(Nullable(Int32), Int32), t2 Tuple(Int32, Int32)', $$\N,5,2,3$$);

-- A one-element Tuple is a separate column too, so its `\N` is that element. Reading it as the whole
-- column instead would apply the column DEFAULT expression, which is what makes the two differ here.
SELECT 'one-element tuple';
SET input_format_defaults_for_omitted_fields = 1;

DROP TABLE IF EXISTS t04405_width_one;
CREATE TABLE t04405_width_one (t Tuple(Nullable(Int32)) DEFAULT tuple(7)) ENGINE = Memory;
INSERT INTO t04405_width_one FORMAT CSV
\N

SELECT * FROM t04405_width_one;
DROP TABLE t04405_width_one;

DROP TABLE IF EXISTS t04405_width_one_not_null;
CREATE TABLE t04405_width_one_not_null (t Tuple(Int32) DEFAULT tuple(7)) ENGINE = Memory;
INSERT INTO t04405_width_one_not_null FORMAT CSV
\N

SELECT * FROM t04405_width_one_not_null;
DROP TABLE t04405_width_one_not_null;

-- An empty Tuple() has no elements, so it occupies a single cell and a `\N` is the whole column.
SELECT 'empty tuple';
SELECT * FROM format(CSV, 'x Tuple(), y UInt8', $$\N,2$$) SETTINGS enable_named_columns_in_function_tuple = 0;

-- A Nullable(Tuple) is written and read as a single CSV field, so a `\N` is the whole column.
SELECT 'nullable(tuple) is a single field';
SELECT * FROM format(CSV, 'x Nullable(Tuple(Nullable(Int32), Int32))', $$\N
"(NULL,2)"$$) ORDER BY x IS NULL, x SETTINGS enable_nullable_tuple_type = 1;

-- Types other than Tuple keep the whole-column null_as_default behaviour.
SELECT 'non-tuple types';
SELECT * FROM format(CSV, 'x Int32', $$\N$$);
SELECT * FROM format(CSV, 'x Array(Int32)', $$\N$$);
SELECT * FROM format(CSV, 'x Nullable(Int32)', $$\N$$);

-- A quoted Tuple with deserialize_separate_columns_into_tuple = 0 is unaffected.
SELECT 'separate columns disabled';
SELECT * FROM format(CSV, 'x Tuple(Nullable(Int32), Int32)', $$"(NULL,1)"$$) SETTINGS input_format_csv_deserialize_separate_columns_into_tuple = 0;

-- Round-trip: what the CSV writer produces is what the reader accepts.
SELECT 'round trip';
SELECT * FROM format(CSV, 'x Tuple(Nullable(Int32), Int32)', $$\N,0$$);
SELECT * FROM format(CSV, 'a UInt8, t Tuple(Nullable(Int32), Int32), b UInt8', $$1,\N,5,2$$);
