-- `groupNumericIndexedVector` accumulates `v[index] += value`. The bit-sliced full adder in
-- `addValue` used to set a bit when the sum bit was 1 but never clear it when the sum bit was 0, so
-- every carry left the lower bit set. Any repeated index whose addends share a set bit was affected:
-- `n` rows of value `1` at one index accumulated to `2^n - 1` instead of `n`.

-- additions that need a carry
SELECT numericIndexedVectorToMap(groupNumericIndexedVectorState(toUInt8(5), val))
FROM (SELECT arrayJoin([toInt64(10), toInt64(10)]) AS val) SETTINGS max_threads = 1;
SELECT numericIndexedVectorToMap(groupNumericIndexedVectorState(toUInt8(5), val))
FROM (SELECT arrayJoin([toInt64(10), toInt64(6)]) AS val) SETTINGS max_threads = 1;
SELECT numericIndexedVectorToMap(groupNumericIndexedVectorState(toUInt8(5), toInt64(1)))
FROM numbers(8) SETTINGS max_threads = 1;

-- additions whose bits are disjoint were already correct, and must stay so
SELECT numericIndexedVectorToMap(groupNumericIndexedVectorState(toUInt8(5), val))
FROM (SELECT arrayJoin([toInt64(10), toInt64(5)]) AS val) SETTINGS max_threads = 1;

-- the sum of all values must match plain aggregation over the same rows, with repeated indexes,
-- negative values and a fractional value type
SELECT numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(idx, val)) = sum(val)
FROM (SELECT toUInt8(number % 7) AS idx, toInt64(number % 11) - 5 AS val FROM numbers(500))
SETTINGS max_threads = 1;
SELECT numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(idx, val)) = sum(val)
FROM (SELECT CAST(number % 9, 'Int8') - 4 AS idx, toFloat64(number % 5) - 2 AS val FROM numbers(500))
SETTINGS max_threads = 1;

-- the row-by-row path must agree with the pointwise path, which adds the same values bitmap-wise
WITH (SELECT groupNumericIndexedVectorState(toUInt8(5), toInt64(7)) FROM numbers(1)) AS a,
     (SELECT groupNumericIndexedVectorState(toUInt8(5), toInt64(-7)) FROM numbers(1)) AS b
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(a, b));
SELECT numericIndexedVectorToMap(groupNumericIndexedVectorState(toUInt8(5), val))
FROM (SELECT arrayJoin([toInt64(7), toInt64(-7)]) AS val) SETTINGS max_threads = 1;

-- an index whose value is driven to zero stays present with value zero, which is the state the
-- pointwise operations produce, and all readers agree on it
SELECT numericIndexedVectorToMap(v) AS map,
       numericIndexedVectorGetValue(v, toUInt8(5)) AS get_value,
       numericIndexedVectorCardinality(v) AS cardinality
FROM (SELECT groupNumericIndexedVectorState(toUInt8(5), val) AS v
      FROM (SELECT arrayJoin([toInt64(5), toInt64(-5)]) AS val))
SETTINGS max_threads = 1;

-- adding zero must not reset an index that already holds a value
SELECT numericIndexedVectorToMap(v) AS map, numericIndexedVectorGetValue(v, toUInt8(5)) AS get_value
FROM (SELECT groupNumericIndexedVectorState(toUInt8(5), val) AS v
      FROM (SELECT arrayJoin([toInt64(3), toInt64(0)]) AS val))
SETTINGS max_threads = 1;

-- negative indexes go through the same write path
SELECT numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(CAST(idx, 'Int8'), toInt64(1)))
FROM (SELECT arrayJoin(arrayConcat(range(1, 33), [-1, -1, -1])) AS idx) SETTINGS max_threads = 1;

-- `numericIndexedVectorBuild` replays every pair of the map through the same write path, and `Map`
-- does not enforce unique keys, so a repeated key accumulates just like a repeated row does
SELECT numericIndexedVectorToMap(numericIndexedVectorBuild(CAST(mapFromArrays([5, 5], [10, 10]), 'Map(UInt8, Int64)')));
SELECT numericIndexedVectorToMap(numericIndexedVectorBuild(CAST(mapFromArrays([5, 5], [7, -7]), 'Map(UInt8, Int64)')));

-- A state written before `addValue` maintained the invariant can carry an index in `zero_indexes`
-- and in the bit slices at the same time, and the readers that answer zero comparisons trust
-- `zero_indexes` alone. Deserialization restores the invariant, so such a state stops reporting a
-- non-zero index as equal to zero. The literal is a state produced by an earlier server for
-- `groupNumericIndexedVectorState(toUInt8(5), val)` over the values 3 and 0.
WITH CAST(unhex('40000000000000000000010500000105000001050101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101'), 'AggregateFunction(groupNumericIndexedVector, UInt8, Int64)') AS v
SELECT numericIndexedVectorToMap(v) AS map,
       numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(v, 0)) AS equals_zero,
       numericIndexedVectorGetValue(v, toUInt8(5)) AS get_value;

-- an index whose value really is zero must still compare equal to zero
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(v, 0))
FROM (SELECT groupNumericIndexedVectorState(toUInt8(5), toInt64(0)) AS v FROM numbers(1));
