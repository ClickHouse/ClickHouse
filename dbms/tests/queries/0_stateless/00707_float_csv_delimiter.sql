CREATE TEMPORARY TABLE test (x Float32, y Float64, z UInt64, s String);

INSERT INTO test FORMAT CSV 123.456,789.012,345678,Hello

SET format_csv_delimiter = ';';
INSERT INTO test FORMAT CSV 123.456;789.012;345678;Hello

SET format_csv_delimiter = ':';
INSERT INTO test FORMAT CSV 123.456:789.012:345678:Hello

SET format_csv_delimiter = '|';
INSERT INTO test FORMAT CSV 123.456|789.012|345678|Hello

SELECT * FROM test;
