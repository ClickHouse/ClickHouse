-- Tags: no-fasttest
-- no-fasttest: requires idna library

SELECT '-- Negative tests';

SELECT punycodeDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeDecode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeEncode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT punycodeDecode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeEncode('two', 'strings'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT punycodeDecode(toFixedString('two', 3)); -- { serverError ILLEGAL_COLUMN }
SELECT punycodeEncode(toFixedString('two', 3)); -- { serverError ILLEGAL_COLUMN }

SELECT '-- Regular cases';

-- The test cases originate from the ada idna unit tests:
--- https://github.com/ada-url/idna/blob/8cd03ef867dbd06be87bd61df9cf69aa1182ea21/tests/fixtures/utf8_punycode_alternating.txt

SELECT 'a' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'A' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT '--' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'London' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'Lloyd-Atkinson' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'This has spaces' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT '-> $1.00 <-' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'а' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'ü' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'α' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT '例' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT '😉' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'αβγ' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'München' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'Mnchen-3ya' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'München-Ost' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'Bahnhof München-Ost' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'abæcdöef' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'правда' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'ยจฆฟคฏข' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'ドメイン名例' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT 'MajiでKoiする5秒前' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT '「bücher」' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;
SELECT '团淄' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;

SELECT '-- Special cases';

SELECT punycodeDecode('');
SELECT punycodeEncode('');
SELECT punycodeDecode(NULL);
SELECT punycodeEncode(NULL);

-- garbage Punycode-encoded values
SELECT punycodeDecode('no punycode'); -- { serverError BAD_ARGUMENTS }

-- long input
SELECT 'Wenn Sie ... vom Hauptbahnhof in München ... mit zehn Minuten, ohne, dass Sie am Flughafen noch einchecken müssen, dann starten Sie im Grunde genommen am Flughafen ... am ... am Hauptbahnhof in München starten Sie Ihren Flug. Zehn Minuten. Schauen Sie sich mal die großen Flughäfen an, wenn Sie in Heathrow in London oder sonst wo, meine se ... Charles de Gaulle äh in Frankreich oder in ...äh... in ... in...äh...in Rom. Wenn Sie sich mal die Entfernungen ansehen, wenn Sie Frankfurt sich ansehen, dann werden Sie feststellen, dass zehn Minuten... Sie jederzeit locker in Frankfurt brauchen, um ihr Gate zu finden. Wenn Sie vom Flug ... vom ... vom Hauptbahnhof starten - Sie steigen in den Hauptbahnhof ein, Sie fahren mit dem Transrapid in zehn Minuten an den Flughafen in ... an den Flughafen Franz Josef Strauß. Dann starten Sie praktisch hier am Hauptbahnhof in München. Das bedeutet natürlich, dass der Hauptbahnhof im Grunde genommen näher an Bayern ... an die bayerischen Städte heranwächst, weil das ja klar ist, weil auf dem Hauptbahnhof viele Linien aus Bayern zusammenlaufen.' AS str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original;

-- non-const values
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (str String) ENGINE=MergeTree ORDER BY str;
INSERT INTO tab VALUES ('abc') ('aäoöuü') ('München');
SELECT str, punycodeEncode(str) AS puny, punycodeDecode(puny) AS original FROM tab;
DROP TABLE tab;
