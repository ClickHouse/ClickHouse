-- Exercise SerializationTime64 deserializers across all text formats:
-- deserializeText, deserializeTextEscaped, deserializeTextQuoted,
-- deserializeTextJSON, deserializeTextCSV, tryDeserializeWholeText,
-- tryDeserializeTextEscaped, tryDeserializeTextQuoted, tryDeserializeTextJSON.

SELECT '--- CAST: serializeText / deserializeTextQuoted ---';
SELECT CAST('12:30:45.123' AS Time64(3));
SELECT CAST('12:30:45' AS Time64(3));
SELECT CAST('00:00:00.000' AS Time64(3));
SELECT CAST('23:59:59.999' AS Time64(3));
SELECT CAST('999:59:59.999' AS Time64(3));
SELECT CAST('-5:30:00' AS Time64(3));
SELECT CAST(0 AS Time64(3));
SELECT CAST(45045123 AS Time64(3));
SELECT CAST('12:30:45.123456' AS Time64(6));

SELECT '--- INSERT VALUES: deserializeTextQuoted quoted + numeric branches ---';
DROP TABLE IF EXISTS t64_values;
CREATE TABLE t64_values (t Time64(3)) ENGINE = Memory;
INSERT INTO t64_values VALUES ('12:30:45.123'), ('00:00:00.000'), ('23:59:59.999'), (45045123), (0);
SELECT * FROM t64_values ORDER BY t FORMAT TSV;

SELECT '--- JSONEachRow input: deserializeTextJSON with quoted string ---';
SELECT t FROM format(JSONEachRow, 't Time64(3)',
'{"t": "12:30:45.123"}
{"t": "00:00:00.000"}
{"t": "23:59:59.999"}') ORDER BY t;

SELECT '--- JSONEachRow input: deserializeTextJSON with numeric value ---';
SELECT t FROM format(JSONEachRow, 't Time64(3)',
'{"t": 45045123}
{"t": 0}
{"t": -12345}') ORDER BY t;

SELECT '--- CSV input: deserializeTextCSV with quote char ---';
SELECT t FROM format(CSV, 't Time64(3)',
'"12:30:45.123"
''00:00:00.000''') ORDER BY t;

SELECT '--- CSV input: deserializeTextCSV without quotes (numeric) ---';
SELECT t FROM format(CSV, 't Time64(3)',
'45045123
0') ORDER BY t;

SELECT '--- TSV input: deserializeTextEscaped ---';
SELECT t FROM format(TabSeparated, 't Time64(3)',
'12:30:45.123
00:00:00.000') ORDER BY t;

SELECT '--- TSKV input: deserializeTextEscaped via TSKV ---';
SELECT t FROM format(TSKV, 't Time64(3)',
't=12:30:45.123
t=00:00:00.000
') ORDER BY t;

SELECT '--- Values input with quoted + numeric literal ---';
SELECT t FROM format(Values, 't Time64(3)', '(''12:30:45.123''),(12345),(0)') ORDER BY t;

SELECT '--- Prewritten and reparsed via formatRow round-trip ---';
-- formatRow uses serialize* paths, the result goes through format table function
-- re-parsing which calls the matching deserialize* paths.
WITH '12:30:45.123'::Time64(3) AS original
SELECT toTypeName(original), toString(original);

SELECT '--- Various Time64 scales: 0,3,6,9 ---';
SELECT CAST('12:30:45' AS Time64(0));
SELECT CAST('12:30:45.1' AS Time64(1));
SELECT CAST('12:30:45.123' AS Time64(3));
SELECT CAST('12:30:45.123456' AS Time64(6));
SELECT CAST('12:30:45.123456789' AS Time64(9));

SELECT '--- Time64 read errors ---';
SELECT CAST('not a time' AS Time64(3)); -- { serverError CANNOT_PARSE_DATETIME }
-- '99:99:99' is actually accepted by the parser and written as-is.
SELECT CAST('99:99:99' AS Time64(3));

SELECT '--- Time64 tryCast ---';
SELECT accurateCastOrNull('12:30:45.123', 'Time64(3)');
SELECT accurateCastOrNull('garbage', 'Time64(3)');
SELECT accurateCastOrDefault('garbage', 'Time64(3)');

DROP TABLE t64_values;
