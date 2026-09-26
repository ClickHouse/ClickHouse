-- Test quoted-float parsing in CSV via the SerializationNumber CSV helpers.
-- Covers both the throwing deserializeNumberCSV path and the non-throwing
-- tryDeserializeNumberCSV closing-quote check.

-- A double-quoted float is accepted (2.5 is exact, so precise/fast agree).
SELECT x FROM format(CSV, 'x Float64', '"2.5"') ORDER BY x;

-- Throwing path: with input_format_csv_use_default_on_bad_values the field is
-- buffered and parsed by the throwing deserializeNumberCSV; the missing closing
-- quote makes assertChar throw, readFieldOrDefault catches it and inserts the
-- column default instead.
SELECT x FROM format(CSV, 'x Float64', '"1.5') ORDER BY x SETTINGS input_format_csv_use_default_on_bad_values = 1;

-- Non-throwing path: a Variant routes the Float64 candidate through
-- tryDeserializeNumberCSV. The unterminated quoted value fails the closing-quote
-- check (SerializationNumber.cpp:77-78), so the Float64 candidate is rejected and
-- the value falls back to String.
SELECT toTypeName(x) AS t, variantElement(x, 'Float64') AS f, variantElement(x, 'String') AS s
FROM format(CSV, 'x Variant(Float64, String)', '"1.5');

-- Control: a well-formed float is still accepted as Float64 through the same path.
SELECT toTypeName(x) AS t, variantElement(x, 'Float64') AS f, variantElement(x, 'String') AS s
FROM format(CSV, 'x Variant(Float64, String)', '"1.5"');
