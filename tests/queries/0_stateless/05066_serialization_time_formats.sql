-- Coverage test for SerializationTime: text format serializers and deserializers.
-- Targets uncovered paths in src/DataTypes/Serializations/SerializationTime.cpp:
--   lines 139-143: serializeTextJSON   (via FORMAT JSONEachRow)
--   lines 146-159: deserializeTextJSON (via format() table function, quoted string and bare integer)
--   lines 195-200: deserializeTextCSV quoted branch (via format() with double-quoted value)
-- Tags: no-random-settings

-- serializeTextJSON: Time value serialized to a JSON string.
SELECT CAST('12:34:56' AS Time) AS t FORMAT JSONEachRow;

-- deserializeTextJSON: parse quoted string and bare integer from JSONEachRow input.
-- Covers the if/else branches at lines 147-156 (quoted path and integer path).
SELECT t FROM format(JSONEachRow, 't Time',
'{"t":"12:34:56"}
{"t":45056}');

-- deserializeTextCSV quoted branch: time string wrapped in CSV double-quotes.
-- Covers lines 195-200 (quoted path inside deserializeTextCSV).
SELECT t FROM format(CSV, 't Time', '"12:34:56"');
