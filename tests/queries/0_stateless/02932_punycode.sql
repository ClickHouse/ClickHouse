-- Tags: no-fasttest
-- no-fasttest: requires idna library
-- Disable force_primary_key_reverse_order: reversed key changes physical row order in tables, affecting SELECT output without explicit ORDER BY
SET force_primary_key_reverse_order = 0;

-- See also 02932_idna.sql

SELECT '-- Negative tests';

SELECT punycodeEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryPunycodeDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT punycodeEncode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeDecode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tryPunycodeDecode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT punycodeEncode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeDecode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tryPunycodeDecode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT punycodeEncode(toFixedString('two', 3)); -- { serverError NOT_IMPLEMENTED }
SELECT punycodeDecode(toFixedString('two', 3)); -- { serverError NOT_IMPLEMENTED }
SELECT tryPunycodeDecode(toFixedString('two', 3)); -- { serverError NOT_IMPLEMENTED }

SELECT '-- Regular cases';

-- The test cases originate from the ada idna unit tests:
-- - https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/utf8_punycode_alternating.txt

SELECT 'a' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'A' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT '--' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'London' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'Lloyd-Atkinson' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'This has spaces' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT '-> $1.00 <-' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'а' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'ü' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'α' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT '例' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT '😉' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'αβγ' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'München' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'Mnchen-3ya' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'München-Ost' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'Bahnhof München-Ost' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'abæcdöef' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'правда' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'ยจฆฟคฏข' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'ドメイン名例' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT 'MajiでKoiする5秒前' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT '「bücher」' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
SELECT '团淄' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try;
--
SELECT '-- Special cases';

SELECT '---- Empty input';
SELECT punycodeEncode('');
SELECT punycodeDecode('');
SELECT tryPunycodeDecode('');

SELECT '---- NULL input';
SELECT punycodeEncode(NULL);
SELECT punycodeDecode(NULL);
SELECT tryPunycodeDecode(NULL);

SELECT '---- Garbage Punycode-encoded input';
SELECT punycodeDecode('no punycode'); -- { serverError BAD_ARGUMENTS }
SELECT tryPunycodeDecode('no punycode');

SELECT '---- Long input';
SELECT 'Wenn Sie ... vom Hauptbahnhof in München ... mit zehn Minuten, ohne, dass Sie am Flughafen noch einchecken müssen, dann starten Sie im Grunde genommen am Flughafen ... am ... am Hauptbahnhof in München starten Sie Ihren Flug. Zehn Minuten. Schauen Sie sich mal die großen Flughäfen an, wenn Sie in Heathrow in London oder sonst wo, meine se ... Charles de Gaulle äh in Frankreich oder in ...äh... in ... in...äh...in Rom. Wenn Sie sich mal die Entfernungen ansehen, wenn Sie Frankfurt sich ansehen, dann werden Sie feststellen, dass zehn Minuten... Sie jederzeit locker in Frankfurt brauchen, um ihr Gate zu finden. Wenn Sie vom Flug ... vom ... vom Hauptbahnhof starten - Sie steigen in den Hauptbahnhof ein, Sie fahren mit dem Transrapid in zehn Minuten an den Flughafen in ... an den Flughafen Franz Josef Strauß. Dann starten Sie praktisch hier am Hauptbahnhof in München. Das bedeutet natürlich, dass der Hauptbahnhof im Grunde genommen näher an Bayern ... an die bayerischen Städte heranwächst, weil das ja klar ist, weil auf dem Hauptbahnhof viele Linien aus Bayern zusammenlaufen.' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try FORMAT Vertical;

SELECT '---- Non-const values';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (str String) ENGINE=MergeTree ORDER BY str;
INSERT INTO tab VALUES ('abc') ('aäoöuü') ('München');
SELECT str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, tryPunycodeDecode(puny) AS original_try FROM tab;
DROP TABLE tab;

SELECT '---- Non-const values with invalid values sprinkled in';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (puny String) ENGINE=MergeTree ORDER BY puny;
INSERT INTO tab VALUES ('Also no punycode') ('London-') ('Mnchen-3ya') ('No punycode') ('Rtting-3ya') ('XYZ no punycode');
SELECT puny, punycodeDecode(puny) AS original FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT puny, tryPunycodeDecode(puny) AS original FROM tab;
DROP TABLE tab;
