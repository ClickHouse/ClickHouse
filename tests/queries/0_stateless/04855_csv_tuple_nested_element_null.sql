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
