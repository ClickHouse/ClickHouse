-- Tags: no-darwin
-- no-darwin: the fast float parser (precise_float_parsing = 0) rounds to different least-significant digits on Darwin.

-- precise_float_parsing must apply to input-format and literal parsing, not only to toFloat*/CAST.
-- Regressions: CSV/input formats, VALUES numeric literals, and scientific notation.
-- The default is the precise (closest-representable) parser; precise_float_parsing = 0 restores
-- the older faster, less accurate one, so every case below differs between the two settings.

DROP TABLE IF EXISTS t_precise_float;
CREATE TABLE t_precise_float (x Float64) ENGINE = Memory;

-- A value read from CSV/TSV/JSON is parsed precisely by default and can be switched to fast.
SELECT '-- CSV --';
SELECT x FROM format(CSV, 'x Float64', '1.6725');                                    -- precise (default): 1.6725
SELECT x FROM format(CSV, 'x Float64', '1.6725') SETTINGS precise_float_parsing = 0; -- fast: 1.6724999999999999
SELECT '-- TSV --';
SELECT x FROM format(TSV, 'x Float64', '1.6725');
SELECT '-- JSONEachRow --';
SELECT x FROM format(JSONEachRow, 'x Float64', '{"x":1.6725}');

-- Scientific notation is parsed as a whole and matches the numeric literal, instead of parsing
-- mantissa and exponent separately (which yields 0.1 * 1e-5 with the fast parser).
SELECT '-- scientific notation --';
SELECT x FROM format(TSV, 'x Float64', '0.1e-5');                                    -- precise (default): 0.000001
SELECT x FROM format(TSV, 'x Float64', '0.1e-5') SETTINGS precise_float_parsing = 0; -- fast: 0.0000010000000000000002
SELECT 0.1e-5 = (SELECT x FROM format(TSV, 'x Float64', '0.1e-5'));                  -- 1: matches the literal

-- An unquoted numeric literal in VALUES respects the setting (it used to ignore it and always be fast).
SELECT '-- VALUES literal --';
INSERT INTO t_precise_float SETTINGS precise_float_parsing = 1 VALUES (1.234567891209E12);
INSERT INTO t_precise_float SETTINGS precise_float_parsing = 0 VALUES (1.234567891209E12);
SELECT toString(x), x = 1234567891209.0 FROM t_precise_float ORDER BY x;

-- Overflow/underflow stays lenient (+-inf / 0), like the fast parser and strtod.
SELECT '-- overflow lenient --';
SELECT x FROM format(TSV, 'x Float64', '1e400\n-1e400\n1e-400');

DROP TABLE t_precise_float;
