-- `groupNumericIndexedVector` sums the values of a repeated index by a ripple-carry addition over the
-- bit slices of the index. A bit that the addition turns off was never cleared, so the stored value
-- became `old | (old + new)` - 5 and then 3 read back as 13 - while merging two states, which adds
-- whole bit slices, was right. The result then depended on how the rows were split across the states.

SELECT 'five and three', numericIndexedVectorToMap(groupNumericIndexedVectorState(1::UInt32, v)) FROM (SELECT arrayJoin([5, 3])::UInt64 AS v) SETTINGS max_threads = 1;
SELECT 'three values', numericIndexedVectorToMap(groupNumericIndexedVectorState(1::UInt32, v)) FROM (SELECT arrayJoin([5, 3, 100])::UInt64 AS v) SETTINGS max_threads = 1;
SELECT 'get value', numericIndexedVectorGetValue(groupNumericIndexedVectorState(1::UInt32, v), 1) FROM (SELECT arrayJoin([5, 3])::UInt64 AS v) SETTINGS max_threads = 1;
SELECT 'all value sum', numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(1::UInt32, v)) FROM (SELECT arrayJoin([5, 3])::UInt64 AS v) SETTINGS max_threads = 1;

-- Every path over the same multiset of rows agrees, including on the wrap-around of a sum that does
-- not fit into the value type.
SELECT 'five rows of 2^62', numericIndexedVectorToMap(groupNumericIndexedVectorState(1::UInt32, 4611686018427387904::UInt64)) FROM numbers(5) SETTINGS max_threads = 1;
SELECT 'merged 3 and 2', numericIndexedVectorToMap(groupNumericIndexedVectorMergeState(s))
FROM (SELECT groupNumericIndexedVectorState(1::UInt32, 4611686018427387904::UInt64) AS s FROM numbers(3)
      UNION ALL
      SELECT groupNumericIndexedVectorState(1::UInt32, 4611686018427387904::UInt64) FROM numbers(2));
SELECT 'merged 4 and 1', numericIndexedVectorToMap(groupNumericIndexedVectorMergeState(s))
FROM (SELECT groupNumericIndexedVectorState(1::UInt32, 4611686018427387904::UInt64) AS s FROM numbers(4)
      UNION ALL
      SELECT groupNumericIndexedVectorState(1::UInt32, 4611686018427387904::UInt64) FROM numbers(1));

SELECT 'int8 wrap', numericIndexedVectorToMap(groupNumericIndexedVectorState(1::UInt32, v)) FROM (SELECT arrayJoin([100, 100])::Int8 AS v) SETTINGS max_threads = 1;
SELECT 'uint8 wrap', numericIndexedVectorToMap(groupNumericIndexedVectorState(1::UInt32, v)) FROM (SELECT arrayJoin([200, 100])::UInt8 AS v) SETTINGS max_threads = 1;
SELECT 'negative and positive', numericIndexedVectorToMap(groupNumericIndexedVectorState(1::UInt32, v)) FROM (SELECT arrayJoin([-5, 3])::Int64 AS v) SETTINGS max_threads = 1;
SELECT 'fractions', numericIndexedVectorToMap(groupNumericIndexedVectorState(1::UInt32, v)) FROM (SELECT arrayJoin([1.5, 2.25])::Float64 AS v) SETTINGS max_threads = 1;

-- An index whose values cancel out keeps a value of zero, the same way a merged state reports it.
SELECT 'added to zero', numericIndexedVectorToMap(groupNumericIndexedVectorState(1::UInt32, v)) FROM (SELECT arrayJoin([5, -5])::Int64 AS v) SETTINGS max_threads = 1;
SELECT 'merged to zero', numericIndexedVectorToMap(groupNumericIndexedVectorMergeState(s))
FROM (SELECT groupNumericIndexedVectorState(1::UInt32, 5::Int64) AS s UNION ALL SELECT groupNumericIndexedVectorState(1::UInt32, -5::Int64));

-- The same aggregation must not depend on how many threads read the rows.
DROP TABLE IF EXISTS t_05210;
CREATE TABLE t_05210 (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05210 SELECT number % 100, number FROM numbers(1000);

SELECT 'single thread', numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(k, v)) FROM t_05210 SETTINGS max_threads = 1;
SELECT 'many threads', numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(k, v)) FROM t_05210 SETTINGS max_threads = 8, max_block_size = 7;
SELECT 'the true sum', sum(v) FROM t_05210;
SELECT 'per index, single thread', numericIndexedVectorGetValue(groupNumericIndexedVectorState(k, v), 7) FROM t_05210 SETTINGS max_threads = 1;
SELECT 'per index, many threads', numericIndexedVectorGetValue(groupNumericIndexedVectorState(k, v), 7) FROM t_05210 SETTINGS max_threads = 8, max_block_size = 7;
SELECT 'the true per index sum', sum(v) FROM t_05210 WHERE k = 7;

-- `zero_indexes` holds exactly the indexes whose accumulated value is zero, which is what merging two
-- states recomputes, and what `numericIndexedVectorPointwiseEqual` against `0` and the signed
-- comparisons answer straight out of. A `0` row for an index that already carries a value must
-- therefore not be recorded there, or those operators contradict the bit slices and, once more, the
-- answer depends on how the rows were split between the states.
DROP TABLE IF EXISTS t_05210_zero;
CREATE TABLE t_05210_zero (i UInt32, k UInt32, v Int64) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_05210_zero VALUES (1, 1, 5), (2, 1, 0), (3, 2, 0), (4, 2, 5), (5, 3, 0), (6, 4, 5), (7, 4, -5), (8, 5, -5), (9, 5, 0);

SELECT 'zero rows, one state', numericIndexedVectorToMap(groupNumericIndexedVectorState(k, v)) FROM t_05210_zero SETTINGS max_threads = 1;
SELECT 'zero rows, merged', numericIndexedVectorToMap(groupNumericIndexedVectorMergeState(s))
FROM (SELECT groupNumericIndexedVectorState(k, v) AS s FROM t_05210_zero GROUP BY i);

SELECT 'equal to zero, one state', numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorState(k, v), 0)) FROM t_05210_zero SETTINGS max_threads = 1;
SELECT 'equal to zero, merged', numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(groupNumericIndexedVectorMergeState(s), 0))
FROM (SELECT groupNumericIndexedVectorState(k, v) AS s FROM t_05210_zero GROUP BY i);

SELECT 'not equal to zero, one state', numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(groupNumericIndexedVectorState(k, v), 0)) FROM t_05210_zero SETTINGS max_threads = 1;
SELECT 'not equal to zero, merged', numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(groupNumericIndexedVectorMergeState(s), 0))
FROM (SELECT groupNumericIndexedVectorState(k, v) AS s FROM t_05210_zero GROUP BY i);

SELECT 'less than zero, one state', numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(groupNumericIndexedVectorState(k, v), 0)) FROM t_05210_zero SETTINGS max_threads = 1;
SELECT 'less than zero, merged', numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(groupNumericIndexedVectorMergeState(s), 0))
FROM (SELECT groupNumericIndexedVectorState(k, v) AS s FROM t_05210_zero GROUP BY i);

DROP TABLE t_05210_zero;
