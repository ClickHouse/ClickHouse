SELECT JSONExtract('{"a": 123456}', 'FixedString(11)');
SELECT JSONExtract('{"a": 123456}', 'FixedString(12)');
SELECT JSONExtract('{"a": "123456"}', 'a', 'FixedString(5)');
SELECT JSONExtract('{"a": "123456"}', 'a', 'FixedString(6)');
SELECT JSONExtract('{"a": 123456}', 'a', 'FixedString(5)');
SELECT JSONExtract('{"a": 123456}', 'a', 'FixedString(6)');
SELECT JSONExtract(materialize('{"a": 131231}'), 'a', 'LowCardinality(FixedString(5))') FROM numbers(2);
SELECT JSONExtract(materialize('{"a": 131231}'), 'a', 'LowCardinality(FixedString(6))') FROM numbers(2);
