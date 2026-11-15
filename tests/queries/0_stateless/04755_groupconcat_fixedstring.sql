-- Values are the row's bytes with trailing zero bytes stripped, as `CAST(FixedString AS String)` produces.
SELECT hex(groupConcat('|')(x)) FROM (SELECT CAST('abc', 'FixedString(3)') AS x FROM numbers(2));
SELECT hex(groupConcat('|')(x)) FROM (SELECT CAST('ab', 'FixedString(5)') AS x FROM numbers(2));
SELECT hex(groupConcat('|')(x)) FROM (SELECT CAST('a\0b\0\0', 'FixedString(5)') AS x FROM numbers(2));
SELECT hex(groupConcat('|')(x)) FROM (SELECT CAST('\0\0\0', 'FixedString(3)') AS x FROM numbers(2));
SELECT hex(groupConcat('|')(x)) FROM (SELECT CAST('', 'FixedString(5)') AS x FROM numbers(2));
SELECT hex(groupConcat('|')(x)) FROM (SELECT CAST(v, 'Nullable(FixedString(3))') AS x FROM (SELECT arrayJoin(['ab', NULL]) AS v));
SELECT hex(groupConcat('|')(x)) FROM (SELECT CAST(CAST('ab', 'FixedString(3)') AS LowCardinality(FixedString(3))) AS x FROM numbers(2));
SELECT hex(groupConcat('|', 2)(x)) FROM (SELECT CAST('ab', 'FixedString(5)') AS x FROM numbers(4));
