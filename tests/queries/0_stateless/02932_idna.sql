-- Tags: no-fasttest
-- no-fasttest: requires idna library

-- See also 02932_punycode.sql

SELECT '-- Negative tests';

SELECT idnaEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT idnaEncodeOrNull(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT idnaDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT idnaEncode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT idnaEncodeOrNull(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT idnaDecode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT idnaEncode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT idnaEncodeOrNull('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT idnaDecode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT idnaEncode(toFixedString('two', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT idnaEncodeOrNull(toFixedString('two', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT idnaDecode(toFixedString('two', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Regular cases';

-- The test cases originate from the ada idna unit tests:
-- - https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/to_ascii_alternating.txt
-- - https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/to_unicode_alternating.txt

SELECT 'straße.de' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT '2001:4860:4860::8888' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'AMAZON' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'aa--' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'a†--' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'ab--c' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT '-†' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT '-x.xn--zca' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'x-.xn--zca' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'x-.ß' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'x..ß' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT '128.0,0.1' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'xn--zca.xn--zca' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'xn--zca.ß' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'x01234567890123456789012345678901234567890123456789012345678901x' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'x01234567890123456789012345678901234567890123456789012345678901x.xn--zca' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT 'x01234567890123456789012345678901234567890123456789012345678901x.ß' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT '01234567890123456789012345678901234567890123456789.01234567890123456789012345678901234567890123456789.01234567890123456789012345678901234567890123456789.01234567890123456789012345678901234567890123456789.0123456789012345678901234567890123456789012345678.x' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;
SELECT '≠' AS idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull;

SELECT 'aa--' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'ab--c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT '-x' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT '' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--1ch' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--dqd20apc' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--gdh' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--80aaa0ahbbeh4c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--3bs854c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--mgb9awbf' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--mgbaam7a8h' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--mgbbh1a71e' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--s7y.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--55qx5d.xn--tckwe' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--4dbrk0ce' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--zckzah' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--p1ai.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--mxahbxey0c.gr' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--h2brj9c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--d1acpjx3f.xn--p1ai' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--q9jyb4c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--sterreich-z7a.at' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--h2breg3eve.xn--h2brj9c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'ejemplo.xn--q9jyb4c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--9t4b11yi5a.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--gk3at1e.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--42c2d9a' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT '1xn--' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--bih.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--4gbrim.xn----rmckbbajlc6dj7bxne2c.xn--wgbh1c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--mgbb9fbpob' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'xn--55qw42g.xn--55qw42g' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT '≠' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;
SELECT 'ファッション.biz' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, idnaEncodeOrNull(unicode) AS originalOrNull;

SELECT '-- Special cases';

SELECT idnaEncode('');
SELECT idnaEncodeOrNull('');
SELECT idnaDecode('');

SELECT idnaEncode(NULL);
SELECT idnaEncodeOrNull(NULL);
SELECT idnaDecode(NULL);

-- garbage IDNA/unicode values, see
-- - https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/to_ascii_invalid.txt
-- only idnaEncode() is tested, idnaDecode() has by definition no invalid input values
SELECT idnaEncode('xn--'); -- { serverError BAD_ARGUMENTS }
SELECT idnaEncodeOrNull('xn--');
SELECT idnaEncode('ﻱa'); -- { serverError BAD_ARGUMENTS }
SELECT idnaEncodeOrNull('ﻱa');
SELECT idnaEncode('xn--a-yoc'); -- { serverError BAD_ARGUMENTS }
SELECT idnaEncodeOrNull('xn--a-yoc');
SELECT idnaEncode('xn--tešla'); -- { serverError BAD_ARGUMENTS }
SELECT idnaEncodeOrNull('xn--tešla');

-- long input
-- SELECT 'Wenn Sie ... vom Hauptbahnhof in München ... mit zehn Minuten, ohne, dass Sie am Flughafen noch einchecken müssen, dann starten Sie im Grunde genommen am Flughafen ... am ... am Hauptbahnhof in München starten Sie Ihren Flug. Zehn Minuten. Schauen Sie sich mal die großen Flughäfen an, wenn Sie in Heathrow in London oder sonst wo, meine se ... Charles de Gaulle äh in Frankreich oder in ...äh... in ... in...äh...in Rom. Wenn Sie sich mal die Entfernungen ansehen, wenn Sie Frankfurt sich ansehen, dann werden Sie feststellen, dass zehn Minuten... Sie jederzeit locker in Frankfurt brauchen, um ihr Gate zu finden. Wenn Sie vom Flug ... vom ... vom Hauptbahnhof starten - Sie steigen in den Hauptbahnhof ein, Sie fahren mit dem Transrapid in zehn Minuten an den Flughafen in ... an den Flughafen Franz Josef Strauß. Dann starten Sie praktisch hier am Hauptbahnhof in München. Das bedeutet natürlich, dass der Hauptbahnhof im Grunde genommen näher an Bayern ... an die bayerischen Städte heranwächst, weil das ja klar ist, weil auf dem Hauptbahnhof viele Linien aus Bayern zusammenlaufen.' AS idna, idnaEncode(idna) AS ascii, idnaEncodeOrNull(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull FORMAT Vertical;

-- non-const values
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (idna String) ENGINE=MergeTree ORDER BY idna;
INSERT INTO tab VALUES ('straße.münchen.de') ('') ('münchen');
SELECT idna, idnaEncode(idna) AS ascii, idnaDecode(ascii) AS original, idnaEncodeOrNull(idna) AS asciiOrNull, idnaDecode(asciiOrNull) AS originalOrNull FROM tab;
DROP TABLE tab;

-- non-const values with a few invalid values for testing the OrNull variants
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (ascii String) ENGINE=MergeTree ORDER BY ascii;
INSERT INTO tab VALUES ('xn--') ('london.co.uk') ('straße.münchen.de') ('xn--tešla') ('microsoft.com') ('xn--');
SELECT ascii, idnaEncode(ascii) AS original FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT ascii, idnaEncodeOrNull(ascii) AS original FROM tab;
DROP TABLE tab;
