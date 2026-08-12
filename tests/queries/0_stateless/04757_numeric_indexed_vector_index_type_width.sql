-- `groupNumericIndexedVector` must not depend on the width of the index type.
-- The bit slices of the BSI representation are `RoaringBitmapWithSmallSet`, which compares
-- elements in the unsigned domain of the element type, so a negative index has to be looked
-- up as its unsigned counterpart rather than sign-extended to the bitmap's storage width.
--
-- Indices 1..32 fill a bit-slice bitmap to `small_set_size`, the first -1 promotes it to a
-- roaring bitmap, and the second -1 is then read back from the promoted bitmap. `Int8`,
-- `Int16` and `Int32` represent all of these indices exactly, so the three columns describe
-- the same vector and must be equal.
SELECT
    (SELECT numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(CAST(idx, 'Int8'), toInt64(1)))
     FROM (SELECT arrayJoin(arrayConcat(range(1, 33), [-1, -1])) AS idx))
  = (SELECT numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(CAST(idx, 'Int32'), toInt64(1)))
     FROM (SELECT arrayJoin(arrayConcat(range(1, 33), [-1, -1])) AS idx)) AS int8_matches_int32,
    (SELECT numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(CAST(idx, 'Int16'), toInt64(1)))
     FROM (SELECT arrayJoin(arrayConcat(range(1, 33), [-1, -1])) AS idx))
  = (SELECT numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(CAST(idx, 'Int32'), toInt64(1)))
     FROM (SELECT arrayJoin(arrayConcat(range(1, 33), [-1, -1])) AS idx)) AS int16_matches_int32
SETTINGS max_threads = 1;

-- `numericIndexedVectorGetValue` must agree with `numericIndexedVectorToMap` for a negative
-- index, in both the small and the promoted representation.
SELECT 'small' AS repr,
       numericIndexedVectorToMap(v)[-1] AS from_map,
       numericIndexedVectorGetValue(v, CAST(-1, 'Int8')) AS from_get_value
FROM (SELECT groupNumericIndexedVectorState(CAST(idx, 'Int8'), toInt64(7)) AS v
      FROM (SELECT arrayJoin([-1]) AS idx));

SELECT 'promoted' AS repr,
       numericIndexedVectorToMap(v)[-1] AS from_map,
       numericIndexedVectorGetValue(v, CAST(-1, 'Int8')) AS from_get_value
FROM (SELECT groupNumericIndexedVectorState(CAST(idx, 'Int8'), toInt64(7)) AS v
      FROM (SELECT arrayJoin(arrayConcat(range(0, 32), [-1])) AS idx));

-- They must also agree after an index first receives the value 0 and then a non-zero one.
SELECT 'zero then non-zero, Int8' AS what,
       numericIndexedVectorToMap(v)[-1] AS from_map,
       numericIndexedVectorGetValue(v, CAST(-1, 'Int8')) AS from_get_value
FROM (SELECT groupNumericIndexedVectorState(CAST(t.1, 'Int8'), t.2) AS v
      FROM (SELECT arrayJoin([tuple(toInt8(-1), toInt64(0)), tuple(toInt8(-1), toInt64(3))]) AS t))
SETTINGS max_threads = 1;

SELECT 'zero then non-zero, UInt8' AS what,
       numericIndexedVectorToMap(v)[5] AS from_map,
       numericIndexedVectorGetValue(v, toUInt8(5)) AS from_get_value
FROM (SELECT groupNumericIndexedVectorState(toUInt8(5), val) AS v
      FROM (SELECT arrayJoin([toInt64(0), toInt64(3)]) AS val))
SETTINGS max_threads = 1;
