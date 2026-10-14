-- 1. Leading-zero decimals must now infer Float64 in the TSV family, as they always did in CSV.
SELECT 'group 1: TSV infers Float64';
DESC format(TSV, '0.0');
DESC format(TSV, '0.5');
DESC format(TSV, '0.000');
DESC format(TSV, '0.');
DESC format(TSV, '000.5');
DESC format(TSV, '08.5');
DESC format(TSV, '00.0');
DESC format(TSV, '0000000.1');
DESC format(TSV, '0.0000000000000000000000001');

-- 2. The same values in CSV, so the CSV/TSV agreement this fixes is asserted and not assumed.
SELECT 'group 2: CSV control, must equal group 1';
DESC format(CSV, '0.0');
DESC format(CSV, '0.5');
DESC format(CSV, '0.000');
DESC format(CSV, '0.');
DESC format(CSV, '000.5');
DESC format(CSV, '08.5');
DESC format(CSV, '00.0');
DESC format(CSV, '0000000.1');
DESC format(CSV, '0.0000000000000000000000001');

-- 3. Zero-padded integers deliberately stay String. Do not "simplify" this group away: with integer
-- inference enabled these values infer an integer type, and the TSV value parser (readIntTextUnsafe)
-- reads the leading '0' as the whole value, so inferring a number would turn a wrong type into a hard
-- parse error on data that reads today. This is the parser-side limitation of issue #5999.
SELECT 'group 3: zero-padded integers stay String';
DESC format(TSV, '00');
DESC format(TSV, '007');
DESC format(TSV, '03242');
DESC format(TSV, '0100');
DESC format(TSV, '09');
DESC format(TSV, '01');
DESC format(TSV, '00000');
DESC format(TSV, '018446744073709551615');

-- 4. Fields that are not numbers at all are unaffected in both formats.
SELECT 'group 4: non-numbers stay String';
DESC format(TSV, '0x10');
DESC format(TSV, '0b101');
DESC format(TSV, '0o7');
DESC format(TSV, '0.1.2');
DESC format(TSV, '0.5f');
DESC format(CSV, '0x10');
DESC format(CSV, '0.1.2');

-- 5. Values that already worked before the fix must not move. The check the fix rewrites only ever
-- looked at the first byte, so any sign or bare fractional dot moved the leading zero out of its reach
-- and these spellings inferred a float on their own. They are pinned here so that stays true, and so a
-- future rewrite of the check cannot start demoting them.
SELECT 'group 5: already-working values unchanged';
DESC format(TSV, '0');
DESC format(TSV, '1.5');
DESC format(TSV, '10.0');
DESC format(TSV, '-0.5');
DESC format(TSV, '-0.0');
DESC format(TSV, '+0.5');
DESC format(TSV, '.5');
DESC format(TSV, '-.5');
DESC format(TSV, '+.5');
DESC format(TSV, '-.0');
DESC format(TSV, '-00.5');
DESC format(TSV, '+00.5');
SELECT 'group 5: and they read back as the inferred type';
SELECT * FROM format(TSV, '-0.5');
SELECT * FROM format(TSV, '+0.5');
SELECT * FROM format(TSV, '.5');
SELECT * FROM format(TSV, '-.5');
SELECT * FROM format(TSV, '+.5');
SELECT * FROM format(TSV, '-.0');
SELECT * FROM format(TSV, '-00.5');
SELECT * FROM format(TSV, '+00.5');
SELECT 'group 5: CSV agrees on all of them';
DESC format(CSV, '-.5');
DESC format(CSV, '+.5');
DESC format(CSV, '-.0');
DESC format(CSV, '-00.5');

-- 6. Date-shaped controls: date inference runs before the leading-zero check, so these cannot move.
SELECT 'group 6: date-shaped controls stay String';
DESC format(TSV, '0001-01-01');
DESC format(TSV, '09:30:00');

-- 7. Round trip: the admitted type must actually be readable, so a type the parser rejects fails loudly.
SELECT 'group 7: round trip through Float64';
SELECT * FROM format(TSV, 'x Float64', '0.0');
SELECT * FROM format(TSV, 'x Float64', '0.5');
SELECT * FROM format(TSV, 'x Float64', '0.000');
SELECT * FROM format(TSV, 'x Float64', '0.');
SELECT * FROM format(TSV, 'x Float64', '000.5');
SELECT * FROM format(TSV, 'x Float64', '08.5');
SELECT * FROM format(TSV, 'x Float64', '00.0');
SELECT * FROM format(TSV, 'x Float64', '0000000.1');
SELECT * FROM format(TSV, 'x Float64', '0.0000000000000000000000001');

-- 8. Silent row loss: a first row inferring String while a later row infers a number makes TSV header
-- auto-detection consume the data row as a column name, so the row disappears from the result.
SELECT 'group 8: no row is lost';
SELECT count() FROM format(TSV, '0.0\n10.5\n2.3');
SELECT count() FROM format(CSV, '0.0\n10.5\n2.3');
SELECT count() FROM format(TSV, '1.5\n10.5\n2.3');
SELECT count() FROM format(CSV, '1.5\n10.5\n2.3');

-- 9. Type merge across rows.
SELECT 'group 9: type merge';
DESC format(TSV, '0.0\n0.5');
DESC format(TSV, '0.5\n1.5');

-- 10. A leading zero no longer forces exponent forms to String. Inference now validates the number it
-- delimited with the same parser the value reader uses, so a malformed exponent such as `0.5e+` is
-- declined by inference itself and a valid one such as `0e5` is admitted. The leading-zero check
-- therefore covers integers only, and TSV agrees with CSV throughout this group.
SELECT 'group 10: valid exponent forms infer Float64 in TSV, as in CSV';
DESC format(TSV, '0e5') SETTINGS input_format_try_infer_exponent_floats = 1;
DESC format(TSV, '0E5') SETTINGS input_format_try_infer_exponent_floats = 1;
DESC format(TSV, '0e-5') SETTINGS input_format_try_infer_exponent_floats = 1;
DESC format(TSV, '0.5e10') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT 'group 10: and they read back as numbers';
SELECT * FROM format(TSV, '0e5') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT * FROM format(TSV, '0.5e10') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT 'group 10: malformed exponent forms stay String, now via inference itself';
DESC format(TSV, '0.5e+') SETTINGS input_format_try_infer_exponent_floats = 1;
DESC format(TSV, '0e+') SETTINGS input_format_try_infer_exponent_floats = 1;
DESC format(TSV, '0.5e-') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT 'group 10: so they still read back verbatim';
SELECT * FROM format(TSV, '0.5e+') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT * FROM format(TSV, '0e+') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT * FROM format(TSV, '0.5e-') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT d, dynamicType(d) FROM format(TSV, 'd Dynamic', '0.5e+') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT d, dynamicType(d) FROM format(TSV, 'd Dynamic', '0e+') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT 'group 10: CSV agrees with TSV, and the malformed form is readable in both';
DESC format(CSV, '0e5') SETTINGS input_format_try_infer_exponent_floats = 1;
DESC format(CSV, '0.5e10') SETTINGS input_format_try_infer_exponent_floats = 1;
DESC format(CSV, '0.5e+') SETTINGS input_format_try_infer_exponent_floats = 1;
SELECT * FROM format(CSV, '0.5e+') SETTINGS input_format_try_infer_exponent_floats = 1;

-- 11. The integer part of the check is derived from the inferred type, so it follows
-- input_format_try_infer_integers: with integer inference disabled, inference yields Float64, the value
-- is readable, and the check no longer applies.
SELECT 'group 11: try_infer_integers = 1 (the default)';
DESC format(TSV, '0.5') SETTINGS input_format_try_infer_integers = 1;
DESC format(TSV, '00') SETTINGS input_format_try_infer_integers = 1;
DESC format(TSV, '007') SETTINGS input_format_try_infer_integers = 1;
DESC format(TSV, '03242') SETTINGS input_format_try_infer_integers = 1;
DESC format(TSV, '018446744073709551615') SETTINGS input_format_try_infer_integers = 1;
SELECT 'group 11: try_infer_integers = 0';
DESC format(TSV, '0.5') SETTINGS input_format_try_infer_integers = 0;
DESC format(TSV, '00') SETTINGS input_format_try_infer_integers = 0;
DESC format(TSV, '007') SETTINGS input_format_try_infer_integers = 0;
DESC format(TSV, '03242') SETTINGS input_format_try_infer_integers = 0;
DESC format(TSV, '0100') SETTINGS input_format_try_infer_integers = 0;
DESC format(TSV, '09') SETTINGS input_format_try_infer_integers = 0;
DESC format(TSV, '01') SETTINGS input_format_try_infer_integers = 0;
DESC format(TSV, '00000') SETTINGS input_format_try_infer_integers = 0;
DESC format(TSV, '018446744073709551615') SETTINGS input_format_try_infer_integers = 0;
SELECT 'group 11: try_infer_integers = 0, the admitted type is readable';
SELECT * FROM format(TSV, 'x Float64', '00');
SELECT * FROM format(TSV, 'x Float64', '007');
SELECT * FROM format(TSV, 'x Float64', '03242');
SELECT * FROM format(TSV, 'x Float64', '0100');
SELECT * FROM format(TSV, 'x Float64', '09');
SELECT * FROM format(TSV, 'x Float64', '01');
SELECT * FROM format(TSV, 'x Float64', '00000');
SELECT * FROM format(TSV, 'x Float64', '018446744073709551615');

-- 12. The other formats sharing the Raw and Escaped escaping rules.
SELECT 'group 12: TSV-family siblings';
DESC format(TSVRaw, '0.0');
DESC format(TSVRaw, '007');
DESC format(TabSeparated, '0.0');
DESC format(TabSeparated, '007');

-- 13. Several leading zeros with a fractional part: the check is about the inferred type, not about
-- having exactly one leading zero.
SELECT 'group 13: multiple leading zeros with a fractional part';
DESC format(TSV, '00.0');
DESC format(TSV, '00.');
DESC format(TSV, '08.5');
DESC format(TSV, '0000000000.5');

-- 14. The Dynamic runtime path: this arm is not schema-inference only, the inferred type is immediately
-- used to deserialize the value.
SELECT 'group 14: Dynamic runtime path';
SELECT d, dynamicType(d) FROM format(TSV, 'd Dynamic', '0.0');
SELECT d, dynamicType(d) FROM format(TSV, 'd Dynamic', '007');
SELECT d, dynamicType(d) FROM format(CSV, 'd Dynamic', '0.0');

-- 15. TSKV also uses the Escaped rule, so a leading-zero decimal now infers Float64 there too. TSKV
-- decodes escape sequences before inferring but parses the original bytes when reading, so a field whose
-- escapes were decoded must keep inferring String. Nested values are out of that guard's reach, so an
-- escaped number inside an array still infers a numeric element type. The escaped forms below use unhex
-- so the exact wire bytes are unambiguous: 783D305C783245350A is `x=0\x2E5` plus a newline.
SELECT 'group 15: TSKV plain values';
DESC format(TSKV, 'x=0.5\n');
SELECT * FROM format(TSKV, 'x=0.5\n');
DESC format(TSKV, 'x=007\n');
SELECT * FROM format(TSKV, 'x=007\n');
SELECT 'group 15: TSKV null representation and escapes';
DESC format(TSKV, 'x=\\N\ty=1\n');
SELECT x IS NULL, y FROM format(TSKV, 'x=\\N\ty=1\n');
SELECT * FROM format(TSKV, 'x=a\\tb\n');
SELECT 'group 15: TSKV decoded escape must stay String and read back';
DESC format(TSKV, unhex('783D305C783245350A'));
SELECT * FROM format(TSKV, unhex('783D305C783245350A'));
SELECT 'group 15: the same holds without a leading zero';
DESC format(TSKV, unhex('783D315C783245350A'));
SELECT * FROM format(TSKV, unhex('783D315C783245350A'));
DESC format(TSKV, unhex('783D315C7833300A'));
SELECT * FROM format(TSKV, unhex('783D315C7833300A'));
SELECT 'group 15: a non-numeric decoded escape is unaffected';
DESC format(TSKV, unhex('783D615C783245620A'));
SELECT * FROM format(TSKV, unhex('783D615C783245620A'));
DESC format(TSKV, unhex('783D305C700A'));
SELECT * FROM format(TSKV, unhex('783D305C700A'));
-- The guard requires both a decoded escape and a numeric inferred type, so a decoded escape in a Date or
-- in a nested value keeps that type. DESC only: both reads fail here and on master alike.
-- 783D323032302D30312D305C7833310A is `x=2020-01-0\x31` plus a newline, 633D5B305C783245355D0A is
-- `c=[0\x2E5]` plus a newline.
SELECT 'group 15: a decoded escape outside the numeric guard keeps its type';
DESC format(TSKV, unhex('783D323032302D30312D305C7833310A'));
DESC format(TSKV, unhex('633D5B305C783245355D0A'));
