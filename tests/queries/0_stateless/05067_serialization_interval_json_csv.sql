-- Coverage test for SerializationInterval JSON and CSV serialization.
-- Targets uncovered paths in src/DataTypes/Serializations/SerializationInterval.cpp:
--   lines 114-126: serializeTextJSON (Numeric and Kusto format variants)
--   lines 129-141: serializeTextCSV  (Numeric and Kusto format variants)
-- Tags: no-random-settings

-- serializeTextJSON Numeric: Interval value written as a plain number in JSON.
SELECT toIntervalSecond(5) AS i FORMAT JSONEachRow;

-- serializeTextJSON Kusto: Interval value written as a quoted timespan string in JSON.
SELECT toIntervalSecond(5) AS i FORMAT JSONEachRow SETTINGS interval_output_format = 'kusto';

-- serializeTextCSV Numeric: Interval value written as a plain number in CSV.
SELECT toIntervalSecond(5) AS i FORMAT CSV;

-- serializeTextCSV Kusto: Interval value written as a quoted timespan string in CSV.
SELECT toIntervalSecond(5) AS i FORMAT CSV SETTINGS interval_output_format = 'kusto';
