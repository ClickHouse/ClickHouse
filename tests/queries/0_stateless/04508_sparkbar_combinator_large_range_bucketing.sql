-- Regression for exact integer bucket indexing in the -Sparkbar combinator.
-- The bucket index must be computed with integer arithmetic. A Float64 computation loses
-- precision once the range or the offset exceeds the 53-bit exact integer range, silently
-- mis-bucketing valid 64-bit keys near a bucket boundary.
--
-- With width = 2 and the range [0, 9007199254753337], the exact midpoint between the two
-- buckets is at offset ceil(9007199254753337 / 2) = 4503599627376669, so the key
-- 4503599627376668 is the last key of bucket 0 (2 * offset = range - 1 < range). But
-- Float64(9007199254753337) rounds to its nearest even double 9007199254753336, which makes
-- the Float64 expression evaluate to bucket 1 instead. Reverting to Float64 fails this test.

SELECT 'Int64 key just below the bucket-1 boundary (must be bucket 0):';
SELECT countSparkbar(2, 0, 9007199254753337)(materialize(toInt64(4503599627376668)));

SELECT 'UInt64 key just below the bucket-1 boundary (must be bucket 0):';
SELECT countSparkbar(2, 0, 9007199254753337)(materialize(toUInt64(4503599627376668)));

SELECT 'First key of bucket 1 (must be bucket 1):';
SELECT countSparkbar(2, 0, 9007199254753337)(materialize(toInt64(4503599627376669)));

SELECT 'The two keys straddling the exact midpoint land in distinct buckets:';
SELECT countSparkbar(2, 0, 9007199254753337)(x)
FROM (SELECT arrayJoin([toInt64(4503599627376668), 4503599627376669]) AS x);
