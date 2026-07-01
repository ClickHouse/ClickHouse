-- Boundary and corner-case coverage for string -> Float32/Float64 parsing, exercising both the
-- fast (precise_float_parsing = 0) and precise (precise_float_parsing = 1) code paths.
-- toFloat*OrNull is used so that invalid inputs yield NULL instead of aborting the query.

DROP TABLE IF EXISTS float_parsing_cases;
CREATE TABLE float_parsing_cases (x String) ENGINE = Memory;

INSERT INTO float_parsing_cases VALUES
-- zero and sign forms
('0')('-0')('+0')('0.0')('-0.0')('00')('000.000')('0e0')('0.0e10')('.0')('0.')
-- small integers and signs
('1')('-1')('+1')('123')('-123')('+123')('007')('000123')
-- simple fractions and dots
('0.5')('.5')('-.5')('+.5')('5.')('100.')('3.14159265358979')('2.718281828459045')
-- scientific notation variants
('1e0')('1e10')('1E10')('1e+10')('1e-10')('1.5e2')('1.5E-2')('6e-09')('6.000000000000001e-9')
('1e')('e5')('1e+')('1e-')('1.0e')('1e1000')('1e-1000')('1E0307')
-- double rounding ties around 2^52 / 2^53 (round-to-nearest-even)
('4503599627370497')('9007199254740992')('9007199254740993')('9007199254740994')('9007199254740995')
('0.49999999999999994')('0.5000000000000001')('1.5')('2.5')
-- double range: max finite, overflow to inf, smallest normal, subnormals, underflow
('1.7976931348623157e308')('1.7976931348623159e308')('1e308')('2e308')('1e309')('1e400')
('2.2250738585072014e-308')('1e-308')('1e-320')('5e-324')('4.9e-324')('1e-324')('1e-400')
-- float32 range: max finite, overflow, smallest subnormal, 2^24 rounding
('3.4028235e38')('3.4028236e38')('3.5e38')('1.4e-45')('7e-46')('1e-45')
('16777216')('16777217')('16777219')('33554435')
-- long integers crossing the 19 / 38 significant-digit dispatch boundaries
('1234567890123456789')('12345678901234567890')('99999999999999999999')
('18446744073709551615')('18446744073709551616')
('12345678901234567890123456789012345678')('123456789012345678901234567890123456789')
('1234567890123456789012345678901234567890')
('100000000000000000000000000000000000000000000000000000000000')
('123456789012345678901234567890123456789012345678901234567890')
('-123456789012345678901234567890123456789012345678901234567890')
-- long fractions and many significant digits
('0.123456789012345678')('0.12345678901234567890123456789')
('3.141592653589793238462643383279502884197')('0.000000000000000000001')
('0.0000000000000000000000000000000000000000001234567890')
-- leading-zero edge cases for the integer fast path
('0000000000000000000000000000000000000001')('00000000000000000000000123456789012345678901234567890123')
-- whitespace and clearly invalid inputs (expected to fail -> NULL on both paths)
('')(' ')('  ')(' 1')('1 ')('abc')('1abc')('1.2.3')('1..2')('1 2')('--1')('++1')('1-')('1+')
('-')('+')('.')('-.')('+.')('0x10')('0b1')('1f')('nan1')('inf2')('e')
-- infinity and NaN spellings
('inf')('Inf')('INF')('+inf')('-inf')('infinity')('Infinity')('-Infinity')
('nan')('NaN')('NAN')('-nan')('+nan');

SELECT 'precise_float_parsing = 0 (fast)';
SELECT x, toFloat64OrNull(x) AS f64, toFloat32OrNull(x) AS f32
FROM float_parsing_cases ORDER BY x
SETTINGS precise_float_parsing = 0;

SELECT 'precise_float_parsing = 1 (precise)';
SELECT x, toFloat64OrNull(x) AS f64, toFloat32OrNull(x) AS f32
FROM float_parsing_cases ORDER BY x
SETTINGS precise_float_parsing = 1;

DROP TABLE float_parsing_cases;
