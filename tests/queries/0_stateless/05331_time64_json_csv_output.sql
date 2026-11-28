-- Test serialization of Time64 values via JSON and CSV output formats.
-- SerializationTime64::serializeTextJSON (src/DataTypes/Serializations/SerializationTime64.cpp:207-211)
-- is never exercised by the stateless suite because all existing Time64 JSON tests only read FROM
-- JSON (deserializeTextJSON, which is covered). Selecting a Time64 column with FORMAT JSONEachRow
-- routes through serializeTextJSON and writes the value as a quoted string (e.g. "12:30:45.123").
-- SerializationTime64::serializeTextCSV (lines 247-251) has the same gap: CSV output of a Time64
-- column writes it as a double-quoted string and was never tested. This test covers both.
-- Regression: if serializeTextJSON were removed the JSON output would stop quoting Time64 values,
-- which would break RFC 8259 round-trip parsers that expect string-typed time fields.

-- serializeTextJSON: Time64 value to JSON string (lines 207-211)
SELECT CAST('12:30:45.123' AS Time64(3)) AS t FORMAT JSONEachRow;
SELECT CAST('00:00:00.000' AS Time64(3)) AS t FORMAT JSONEachRow;
SELECT CAST('23:59:59.999' AS Time64(3)) AS t FORMAT JSONEachRow;
SELECT CAST('12:30:45' AS Time64(0)) AS t FORMAT JSONEachRow;
SELECT CAST('12:30:45.123456' AS Time64(6)) AS t FORMAT JSONEachRow;

-- serializeTextCSV: Time64 value as double-quoted CSV field (lines 247-251)
SELECT CAST('12:30:45.123' AS Time64(3)) AS t FORMAT CSV;
SELECT CAST('00:00:00.000' AS Time64(3)) AS t FORMAT CSV;
SELECT CAST('23:59:59.999' AS Time64(3)) AS t FORMAT CSV;
SELECT CAST('12:30:45.123456' AS Time64(6)) AS t FORMAT CSV;
