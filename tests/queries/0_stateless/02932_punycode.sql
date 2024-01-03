-- Tags: no-fasttest
-- no-fasttest: requires idna library

SELECT '-- Negative tests';

SELECT punycodeDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeDecodeOrNull(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeEncodeOrNull(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT punycodeDecode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeDecodeOrNull(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeEncode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeEncodeOrNull(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT punycodeDecode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeDecodeOrNull('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeEncode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeEncodeOrNull('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT punycodeDecode(toFixedString('two', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeDecodeOrNull(toFixedString('two', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeEncode(toFixedString('two', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeEncodeOrNull(toFixedString('two', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Regular cases';

-- The test cases originate from the ada idna unit tests:
--- https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/utf8_punycode_alternating.txt

SELECT 'a' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'A' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT '--' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'London' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'Lloyd-Atkinson' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'This has spaces' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT '-> $1.00 <-' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'а' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'ü' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'α' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT '例' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT '😉' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'αβγ' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'München' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'Mnchen-3ya' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'München-Ost' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'Bahnhof München-Ost' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'abæcdöef' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'правда' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'ยจฆฟคฏข' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'ドメイン名例' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT 'MajiでKoiする5秒前' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT '「bücher」' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;
SELECT '团淄' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) AS punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;

SELECT '-- Special cases';

SELECT punycodeDecode('');
SELECT punycodeDecodeOrNull('');
SELECT punycodeEncode('');
SELECT punycodeEncodeOrNull('');

SELECT punycodeDecode(NULL);
SELECT punycodeDecodeOrNull(NULL);
SELECT punycodeEncode(NULL);
SELECT punycodeEncodeOrNull(NULL);

-- garbage Punycode-encoded values
SELECT punycodeDecode('no punycode'); -- { serverError BAD_ARGUMENTS }
SELECT punycodeDecodeOrNull('no punycode');

-- long input
SELECT 'Wenn Sie ... vom Hauptbahnhof in München ... mit zehn Minuten, ohne, dass Sie am Flughafen noch einchecken müssen, dann starten Sie im Grunde genommen am Flughafen ... am ... am Hauptbahnhof in München starten Sie Ihren Flug. Zehn Minuten. Schauen Sie sich mal die großen Flughäfen an, wenn Sie in Heathrow in London oder sonst wo, meine se ... Charles de Gaulle äh in Frankreich oder in ...äh... in ... in...äh...in Rom. Wenn Sie sich mal die Entfernungen ansehen, wenn Sie Frankfurt sich ansehen, dann werden Sie feststellen, dass zehn Minuten... Sie jederzeit locker in Frankfurt brauchen, um ihr Gate zu finden. Wenn Sie vom Flug ... vom ... vom Hauptbahnhof starten - Sie steigen in den Hauptbahnhof ein, Sie fahren mit dem Transrapid in zehn Minuten an den Flughafen in ... an den Flughafen Franz Josef Strauß. Dann starten Sie praktisch hier am Hauptbahnhof in München. Das bedeutet natürlich, dass der Hauptbahnhof im Grunde genommen näher an Bayern ... an die bayerischen Städte heranwächst, weil das ja klar ist, weil auf dem Hauptbahnhof viele Linien aus Bayern zusammenlaufen.' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) as punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull;

-- non-const values
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (str String) ENGINE=MergeTree ORDER BY str;
INSERT INTO tab VALUES ('abc') ('aäoöuü') ('München');
SELECT str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original, punycodeEncodeOrNull(str) as punyOrNull, punycodeDecodeOrNull(punyOrNull) as originalOrNull FROM tab;
DROP TABLE tab;

-- non-const values with a few invalid values for testing the OrNull variants
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (puny String) ENGINE=MergeTree ORDER BY puny;
INSERT INTO tab VALUES ('Also no punycode') ('London-') ('Mnchen-3ya') ('No punycode') ('Rtting-3ya') ('XYZ no punycode');
SELECT puny, punycodeDecode(puny) AS original FROM tab; -- { serverError BAD_ARGUMENTS }
SELECT puny, punycodeDecodeOrNull(puny) AS original FROM tab;
DROP TABLE tab;
