-- Inference must not propose a numeric type the value parser cannot read back.

-- Group 2 merges a rejected number with a later numeric row, which only resolves while numbers are
-- allowed to become strings. That default flipped in 23.9, so a randomized `compatibility` below it
-- would turn those rows into Code 53 instead of the merge under test. At the real default this is a
-- no-op, so the reference is unaffected.
set input_format_json_read_numbers_as_strings = 1;

SELECT '-- 1. JSON rejects a malformed number as a complete String';
DESC format(JSONEachRow, '{"a":1e+}');
DESC format(JSONEachRow, '{"a":+}');
DESC format(JSONEachRow, '{"a":.}');

SELECT '-- 2. a later numeric row must not resurrect the rejected type, in either order';
DESC format(JSONEachRow, '{"a":1e+}\n{"a":1.5}');
DESC format(JSONEachRow, '{"a":1.5}\n{"a":1e+}');
DESC format(JSONEachRow, '{"a":+}\n{"a":1.5}');
DESC format(JSONEachRow, '{"a":[1e+]}\n{"a":[1.5]}');
DESC format(JSONCompactEachRow, '[1e+]\n[1.5]');
-- the merged schema reads; the malformed row stays skippable, which an inference-time throw is not
SELECT a FROM format(JSONEachRow, '{"a":1e+}\n{"a":1.5}') SETTINGS input_format_allow_errors_num = 1;
SELECT a FROM format(JSONEachRow, '{"a":[1e+]}\n{"a":[1.5]}') SETTINGS input_format_allow_errors_num = 1;
-- control: only the malformed token is rejected, not every non-numeric contribution
DESC format(JSONEachRow, '{"a":null}\n{"a":1.5}');
SELECT a FROM format(JSONEachRow, '{"a":null}\n{"a":1.5}') ORDER BY a NULLS FIRST;

SELECT '-- 3. a digit-only value overflowing both integer types keeps its exact digits';
DESC format(TSV, '12345678901234567890123');
DESC format(JSONEachRow, '{"a":12345678901234567890123}');
SELECT c1 FROM format(TSV, '12345678901234567890123');
SELECT a FROM format(JSONEachRow, '{"a":12345678901234567890123}');
-- must not move: these still fit an integer type
DESC format(TSV, '12345678901234567890');
DESC format(TSV, '9223372036854775808');
DESC format(TSV, '1');
DESC format(TSV, '-3');

SELECT '-- 4. the same validation applies to a number inferred from a quoted string';
DESC format(JSONEachRow, '{"a":"1e+"}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1;
-- `1e+` only reaches the permissive delimiter while the exponent setting is on, so the setting is
-- part of this scenario; the JSON twin above needs no pin, because `is_json` forces the exponent
-- grammar unconditionally.
DESC format(CSV, '"1e+"') SETTINGS input_format_csv_try_infer_numbers_from_strings = 1, input_format_try_infer_exponent_floats = 1;
DESC format(JSONEachRow, '{"a":"1.5"}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1;
DESC format(CSV, '"1.5"') SETTINGS input_format_csv_try_infer_numbers_from_strings = 1;

SELECT '-- 5. default-reachable carriers in TSV and CSV, with a read-back';
DESC format(TSV, '.');
DESC format(TSV, '+');
DESC format(TSV, '-');
DESC format(CSV, '.');
DESC format(CSV, '+');
DESC format(CSV, '-');
SELECT c1 FROM format(TSV, '.');
SELECT c1 FROM format(TSV, '+');
SELECT c1 FROM format(CSV, '.');

SELECT '-- 6. on a Dynamic column the divergence was an error at INSERT time';
-- `1e+` only reaches the permissive delimiter while the exponent setting is on, so the setting is
-- part of this scenario and is pinned per statement rather than for the whole file.
SELECT d, dynamicType(d) FROM format(TSV, 'd Dynamic', '1e+') SETTINGS input_format_try_infer_exponent_floats = 1;
-- `.` carries the same divergence at stock settings, so the group keeps a live assertion either way.
SELECT d, dynamicType(d) FROM format(TSV, 'd Dynamic', '.');

SELECT '-- 7. a collection with a trailing delimiter keeps the type it always had';
-- The delimiting pass reports success without consuming anything for the empty element a trailing
-- delimiter leaves behind. Requiring the whole span to be consumed would refuse it, but the value
-- parser accepts that text, so refusing it would be a new divergence in the opposite direction: on
-- a Dynamic column an incomplete type here is absorbed by an existing integer variant, which
-- replaces the whole field with a zero that was never in the input. So the empty element is left to
-- the integer parsers, exactly as before this fix.
-- CAST to Dynamic only runs inference while cast_string_to_dynamic_use_inference is on, so the
-- setting is part of this scenario and is pinned per statement.
SELECT dynamicType(CAST('(1, 2, )', 'Dynamic')) SETTINGS enable_dynamic_type = 1, cast_string_to_dynamic_use_inference = 1;
SELECT dynamicType(CAST('[1, ]', 'Dynamic')) SETTINGS enable_dynamic_type = 1, cast_string_to_dynamic_use_inference = 1;
-- the value parser reads back every type inferred above, which is the property under test
SELECT CAST('(1, 2, )', 'Tuple(UInt8, UInt8, UInt8)'), CAST('[1, ]', 'Array(UInt8)');
-- an existing integer variant must not swallow the whole tuple, which is what a rejection here costs
DROP TABLE IF EXISTS t04652;
CREATE TABLE t04652 (c0 Dynamic) ENGINE = Memory SETTINGS enable_dynamic_type = 1;
INSERT INTO TABLE t04652 (c0) VALUES (1), ((FALSE, FALSE, 'was', ));
SELECT c0, dynamicType(c0) FROM t04652 ORDER BY toString(c0);
DROP TABLE t04652;
-- A failed `true`/`false`/`null` probe also leaves nothing for the number parser, but it walked past
-- the characters it matched, so that leftover is refused rather than preserved. This is the opposite
-- answer to the empty element above, and the two are told apart by whether the field was read at all.
DESC format(JSONEachRow, '{"a":tru}');
DESC format(JSONEachRow, '{"a":nul}') SETTINGS input_format_try_infer_integers = 0;
