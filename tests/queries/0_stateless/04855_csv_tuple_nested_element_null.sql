-- Test a `\N` CSV field that belongs to a nested `Tuple` element of a `Tuple` read from separate columns:
-- it is the whole nested element, so the row is one field shorter and the columns after it still line up.

SET input_format_csv_deserialize_separate_columns_into_tuple = 1;
SET input_format_null_as_default = 1;

SELECT 'nested tuple element';
SELECT * FROM format(CSV, 'x Tuple(Tuple(Int32, Int32), Int32)', $$\N,3$$);

SELECT 'column after the tuple';
SELECT * FROM format(CSV, 'x Tuple(Tuple(Int32, Int32), Int32), y UInt8', $$\N,3,7$$);

SELECT 'both widths in one input';
SELECT * FROM format(CSV, 'x Tuple(Tuple(Int32, Int32), Int32), y UInt8', $$1,2,3,7
\N,3,8$$) ORDER BY y;

SELECT 'Nullable nested tuple element';
SELECT * FROM format(CSV, 'x Tuple(Nullable(Tuple(Int32, Int32)), Int32)', $$\N,3$$) SETTINGS enable_nullable_tuple_type = 1;

-- A `\N` standing for a whole *flat* `Tuple` element, with a column on either side. Reading it as
-- separate columns needs the two tuple fields, so one `\N` cannot fill the element and the row is
-- rejected; turning the separate-columns reading off parses `\N` as the element default instead.
-- Unlike the nested shapes above, this one is not new coverage of the same behaviour: it used to
-- return `1 (0,0) 2` under the default settings and now throws, so it pins that change.
SELECT 'flat tuple element, columns on both sides';
SELECT * FROM format(CSV, 'a UInt8, t Tuple(Int32, Int32), b UInt8', $$1,\N,2$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(CSV, 'a UInt8, t Tuple(Int32, Int32), b UInt8', $$1,\N,2$$) SETTINGS input_format_csv_deserialize_separate_columns_into_tuple = 0;

-- Negative control (invariant to the fix): the whole-column short-circuit is guarded by
-- `null_as_default`, so with it off the `\N` is parsed as tuple text and rejected, for a nested
-- and a flat element alike.
SELECT 'null_as_default = 0 rejects both';
SELECT * FROM format(CSV, 'x Tuple(Tuple(Int32, Int32), Int32)', $$\N,3$$) SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(CSV, 'x Tuple(Int32, Int32)', $$\N,3$$) SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
