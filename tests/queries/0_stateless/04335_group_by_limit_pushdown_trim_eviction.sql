-- Correctness of the top-K heap under heavy eviction, across aggregation methods.
-- Keys arrive in descending order while ORDER BY is ascending, so every new key
-- displaces the heap boundary and the trim/eviction path (including hash-table
-- pruning and aggregate-state destruction) runs constantly.  Each key appears in
-- two passes; rows of the second pass arrive after the key was evicted, so a
-- wrong eviction (erasing the wrong hash-table entry or destroying a live state)
-- shows up as a wrong aggregate value for the surviving keys.

SET enable_group_by_top_k_optimization = 1;
SET max_threads = 1;

SELECT 'UInt32 key (key32)';
SELECT k, count(), sum(v) FROM (SELECT toUInt32(999 - (number % 1000)) AS k, number AS v FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 10;

SELECT 'UInt32 key, DESC';
SELECT k, count() FROM (SELECT toUInt32(number % 1000) AS k FROM numbers(2000)) GROUP BY k ORDER BY k DESC LIMIT 10;

SELECT 'UInt64 key (key64)';
SELECT k, count() FROM (SELECT toUInt64(999 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 10;

SELECT 'Int32 key with negatives';
SELECT k, count() FROM (SELECT toInt32(499 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 10;

SELECT 'Float64 key with nan';
SELECT k, count() FROM (SELECT if(number % 1000 = 500, nan, toFloat64(999 - (number % 1000))) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 10;

SELECT 'Float64 key with nan, DESC';
SELECT k, count() FROM (SELECT if(number % 1000 = 500, nan, toFloat64(number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k DESC LIMIT 3;

SELECT 'Float32 key';
SELECT k, count() FROM (SELECT toFloat32(999 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;

SELECT 'DateTime key';
SELECT k, count() FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + (999 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;

SELECT 'Date key (key16, no hash-table pruning)';
SELECT k, count() FROM (SELECT toDate('2020-01-01') + (999 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;

SELECT 'String key';
SELECT k, count() FROM (SELECT concat('key_', leftPad(toString(999 - (number % 1000)), 4, '0')) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;

SELECT 'FixedString key';
SELECT k, count() FROM (SELECT toFixedString(leftPad(toString(999 - (number % 1000)), 4, '0'), 4) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;

SELECT 'LowCardinality(String) key';
SELECT k, count() FROM (SELECT toLowCardinality(concat('key_', leftPad(toString(999 - (number % 1000)), 4, '0'))) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;

SELECT 'Nullable(UInt32) key, NULLS LAST (null slot is evicted)';
SELECT k, count() FROM (SELECT if(number % 1000 = 500, NULL, toNullable(toUInt32(999 - (number % 1000)))) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC NULLS LAST LIMIT 5;

SELECT 'Nullable(UInt32) key, NULLS FIRST (null slot stays in the heap)';
SELECT k, count() FROM (SELECT if(number % 1000 = 500, NULL, toNullable(toUInt32(999 - (number % 1000)))) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 5;

SELECT 'Tuple key (single serialized GROUP BY column)';
SELECT k, count() FROM (SELECT (toUInt32(999 - (number % 1000)), toString(number % 2)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;

SELECT 'Composite fixed key (UInt32, UInt16)';
SELECT a, b, count() FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toUInt16(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 10;

SELECT 'Composite serialized key (UInt32, String)';
SELECT a, b, count() FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toString(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 10;

SELECT 'Composite nullable key (Nullable(UInt32), String)';
SELECT a, b, count() FROM (SELECT if(number % 1000 = 995, NULL, toNullable(toUInt32(99 - intDiv(number % 1000, 10)))) AS a, toString(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC NULLS LAST, b ASC LIMIT 10;

SELECT 'Composite LowCardinality key (LowCardinality(String), UInt32)';
SELECT a, b, count() FROM (SELECT toLowCardinality(leftPad(toString(99 - intDiv(number % 1000, 10)), 3, '0')) AS a, toUInt32(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 10;

SELECT 'Prefix mode (ORDER BY is a prefix of GROUP BY, no hash-table pruning)';
SELECT * FROM (SELECT a, b, count() FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toUInt16(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC LIMIT 10) ORDER BY a, b;

SELECT 'Stateful aggregate under eviction (uniqExact)';
SELECT k, uniqExact(v) FROM (SELECT toUInt32(999 - (number % 1000)) AS k, number % 3 AS v FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;
