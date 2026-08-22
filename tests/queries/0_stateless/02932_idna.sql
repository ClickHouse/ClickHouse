-- Tags: no-fasttest
-- no-fasttest: requires idna library

-- See also 02932_punycode.sql

SELECT '-- Negative tests';

SELECT idnaEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryIdnaEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT idnaDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT idnaEncode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tryIdnaEncode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT idnaDecode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT idnaEncode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryIdnaEncode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT idnaDecode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT idnaEncode(toFixedString('two', 3)); -- { serverError NOT_IMPLEMENTED }
SELECT tryIdnaEncode(toFixedString('two', 3)); -- { serverError NOT_IMPLEMENTED }
SELECT idnaDecode(toFixedString('two', 3)); -- { serverError NOT_IMPLEMENTED }

SELECT '-- Regular cases';

-- The test cases originate from the ada idna unit tests:
-- - https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/to_ascii_alternating.txt
-- - https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/to_unicode_alternating.txt
--
SELECT 'straße.de' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT '2001:4860:4860::8888' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'AMAZON' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'aa--' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'a†--' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'ab--c' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT '-†' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT '-x.xn--zca' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'x-.xn--zca' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'x-.ß' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'x..ß' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT '128.0,0.1' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'xn--zca.xn--zca' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'xn--zca.ß' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'x01234567890123456789012345678901234567890123456789012345678901x' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'x01234567890123456789012345678901234567890123456789012345678901x.xn--zca' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT 'x01234567890123456789012345678901234567890123456789012345678901x.ß' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT '01234567890123456789012345678901234567890123456789.01234567890123456789012345678901234567890123456789.01234567890123456789012345678901234567890123456789.01234567890123456789012345678901234567890123456789.0123456789012345678901234567890123456789012345678.x' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;
SELECT '≠' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(idna) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try;

SELECT 'aa--' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'ab--c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT '-x' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT '' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--1ch' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--dqd20apc' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--gdh' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--80aaa0ahbbeh4c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--3bs854c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--mgb9awbf' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--mgbaam7a8h' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--mgbbh1a71e' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--s7y.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--55qx5d.xn--tckwe' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--4dbrk0ce' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--zckzah' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--p1ai.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--mxahbxey0c.gr' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--h2brj9c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--d1acpjx3f.xn--p1ai' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--q9jyb4c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--sterreich-z7a.at' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--h2breg3eve.xn--h2brj9c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'ejemplo.xn--q9jyb4c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--9t4b11yi5a.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--gk3at1e.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--42c2d9a' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT '1xn--' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--bih.com' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--4gbrim.xn----rmckbbajlc6dj7bxne2c.xn--wgbh1c' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--mgbb9fbpob' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'xn--55qw42g.xn--55qw42g' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT '≠' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
SELECT 'ファッション.biz' AS ascii, idnaDecode(ascii) AS unicode, idnaEncode(unicode) AS original, tryIdnaEncode(unicode) AS original_try;
--
SELECT '-- Special cases';

SELECT '---- Empty input';
SELECT idnaEncode('');
SELECT tryIdnaEncode('');
SELECT idnaDecode('');

SELECT '---- NULL input';
SELECT idnaEncode(NULL);
SELECT tryIdnaEncode(NULL);
SELECT idnaDecode(NULL);

SELECT '---- Garbage inputs for idnaEncode';
-- - https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/to_ascii_invalid.txt
SELECT idnaEncode('xn--'); -- { serverError BAD_ARGUMENTS }
SELECT tryIdnaEncode('xn--');
SELECT idnaEncode('ﻱa'); -- { serverError BAD_ARGUMENTS }
SELECT tryIdnaEncode('ﻱa');
SELECT idnaEncode('xn--a-yoc'); -- { serverError BAD_ARGUMENTS }
SELECT tryIdnaEncode('xn--a-yoc');
SELECT idnaEncode('xn--tešla'); -- { serverError BAD_ARGUMENTS }
SELECT tryIdnaEncode('xn--tešla');

SELECT '---- Long input';
SELECT 'Wenn Sie ... vom Hauptbahnhof in München ... mit zehn Minuten, ohne, dass Sie am Flughafen noch einchecken müssen, dann starten Sie im Grunde genommen am Flughafen ... am ... am Hauptbahnhof in München starten Sie Ihren Flug. Zehn Minuten. Schauen Sie sich mal die großen Flughäfen an, wenn Sie in Heathrow in London oder sonst wo, meine se ... Charles de Gaulle äh in Frankreich oder in ...äh... in ... in...äh...in Rom. Wenn Sie sich mal die Entfernungen ansehen, wenn Sie Frankfurt sich ansehen, dann werden Sie feststellen, dass zehn Minuten... Sie jederzeit locker in Frankfurt brauchen, um ihr Gate zu finden. Wenn Sie vom Flug ... vom ... vom Hauptbahnhof starten - Sie steigen in den Hauptbahnhof ein, Sie fahren mit dem Transrapid in zehn Minuten an den Flughafen in ... an den Flughafen Franz Josef Strauß. Dann starten Sie praktisch hier am Hauptbahnhof in München. Das bedeutet natürlich, dass der Hauptbahnhof im Grunde genommen näher an Bayern ... an die bayerischen Städte heranwächst, weil das ja klar ist, weil auf dem Hauptbahnhof viele Linien aus Bayern zusammenlaufen.' AS idna, idnaEncode(idna) AS ascii, tryIdnaEncode(ascii) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try FORMAT Vertical;

SELECT '---- Non-const input';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (idna String) ENGINE=MergeTree ORDER BY idna;
INSERT INTO tab VALUES ('straße.münchen.de') ('') ('münchen');
SELECT idna, idnaEncode(idna) AS ascii, tryIdnaEncode(ascii) AS ascii_try, idnaDecode(ascii) AS original, idnaDecode(ascii_try) AS original_try FROM tab;
DROP TABLE tab;

SELECT '---- Non-const input with invalid values sprinkled in';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (idna String) ENGINE=MergeTree ORDER BY idna;
INSERT INTO tab VALUES ('xn--') ('london.co.uk') ('ytraße.münchen.de') ('xn--tešla') ('microsoft.com') ('xn--');
SELECT idna, idnaEncode(idna) AS ascii FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT idna, tryIdnaEncode(idna) AS ascii, idnaDecode(ascii) AS original FROM tab;
DROP TABLE tab;
