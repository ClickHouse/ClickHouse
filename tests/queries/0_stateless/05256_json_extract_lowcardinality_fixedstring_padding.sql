-- A JSON string shorter than the FixedString width is zero padded before it reaches the
-- LowCardinality dictionary: single byte values, empty values and widths above 64.

SELECT hex(JSONExtract('{"a":"x"}', 'a', 'LowCardinality(FixedString(4))'));
SELECT hex(JSONExtract('{"a":""}', 'a', 'LowCardinality(FixedString(4))'));
SELECT hex(JSONExtract(materialize('{"a":"x"}'), 'a', 'LowCardinality(FixedString(2))')) FROM numbers(2);
SELECT JSONExtract('{"a":"x"}', 'a', 'LowCardinality(FixedString(64))') = toFixedString('x', 64);
SELECT JSONExtract('{"a":"x"}', 'a', 'LowCardinality(FixedString(65))') = toFixedString('x', 65);
SELECT JSONExtract(materialize('{"a":"hello"}'), 'a', 'LowCardinality(FixedString(100))') = toFixedString('hello', 100) FROM numbers(2);
