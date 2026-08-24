-- Every row prints 1. `hasAll(set, subset)` is compared against a brute-force
-- equivalent built from `has`, so the expected output does not encode the
-- answers and the test cannot pass by matching a stale reference.
--
-- The array lengths straddle the aarch64 kernel's entry threshold
-- (max(32, 4 * lanes): 64 elements for 1-byte types, 32 for the rest) and its
-- two-vectors-per-reduction and single-vector loop bounds, so a fencepost at
-- any loop handover shows up here.

DROP TABLE IF EXISTS t_has_all;

CREATE TABLE t_has_all (id UInt32, n UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_has_all SELECT number, number FROM numbers(200);

-- Values 0..n-1 as the haystack, needles taken from it, so a length change
-- alone moves the match depth across the whole scan.
WITH
    lengths AS (SELECT arrayJoin([1, 2, 3, 4, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 200]) AS n),
    shapes AS (
        SELECT
            n,
            range(n) AS hay,
            arrayJoin([
                [0],                                    -- first element
                [toUInt64(n - 1)],                      -- last element
                [toUInt64(intDiv(n, 2))],               -- middle
                [toUInt64(n)],                          -- absent
                range(n),                               -- all of itself
                arrayReverse(range(n)),                 -- all, reverse order
                [] :: Array(UInt64)                      -- empty needle list
            ]) AS needles
        FROM lengths
    )
SELECT DISTINCT
    hasAll(arrayMap(x -> toInt8(x % 100), hay), arrayMap(x -> toInt8(x % 100), needles))
        = (length(arrayFilter(y -> NOT has(arrayMap(x -> toInt8(x % 100), hay), y), arrayMap(x -> toInt8(x % 100), needles))) = 0)
    AND hasAll(arrayMap(x -> toInt16(x), hay), arrayMap(x -> toInt16(x), needles))
        = (length(arrayFilter(y -> NOT has(arrayMap(x -> toInt16(x), hay), y), arrayMap(x -> toInt16(x), needles))) = 0)
    AND hasAll(arrayMap(x -> toInt32(x), hay), arrayMap(x -> toInt32(x), needles))
        = (length(arrayFilter(y -> NOT has(arrayMap(x -> toInt32(x), hay), y), arrayMap(x -> toInt32(x), needles))) = 0)
    AND hasAll(arrayMap(x -> toInt64(x), hay), arrayMap(x -> toInt64(x), needles))
        = (length(arrayFilter(y -> NOT has(arrayMap(x -> toInt64(x), hay), y), arrayMap(x -> toInt64(x), needles))) = 0)
    AND hasAll(arrayMap(x -> toUInt8(x % 100), hay), arrayMap(x -> toUInt8(x % 100), needles))
        = (length(arrayFilter(y -> NOT has(arrayMap(x -> toUInt8(x % 100), hay), y), arrayMap(x -> toUInt8(x % 100), needles))) = 0)
    AND hasAll(arrayMap(x -> toUInt16(x), hay), arrayMap(x -> toUInt16(x), needles))
        = (length(arrayFilter(y -> NOT has(arrayMap(x -> toUInt16(x), hay), y), arrayMap(x -> toUInt16(x), needles))) = 0)
    AND hasAll(arrayMap(x -> toUInt32(x), hay), arrayMap(x -> toUInt32(x), needles))
        = (length(arrayFilter(y -> NOT has(arrayMap(x -> toUInt32(x), hay), y), arrayMap(x -> toUInt32(x), needles))) = 0)
    AND hasAll(arrayMap(x -> toUInt64(x), hay), arrayMap(x -> toUInt64(x), needles))
        = (length(arrayFilter(y -> NOT has(arrayMap(x -> toUInt64(x), hay), y), arrayMap(x -> toUInt64(x), needles))) = 0)
    AS all_widths_agree
FROM shapes;

-- hasAny must be unaffected: the kernel is selected only for hasAll.
WITH
    lengths AS (SELECT arrayJoin([1, 16, 31, 32, 33, 64, 65, 200]) AS n),
    shapes AS (
        SELECT n, range(n) AS hay,
               arrayJoin([[0], [toUInt64(n - 1)], [toUInt64(n)], range(n)]) AS needles
        FROM lengths
    )
SELECT DISTINCT
    hasAny(arrayMap(x -> toInt16(x), hay), arrayMap(x -> toInt16(x), needles))
        = (length(arrayFilter(y -> has(arrayMap(x -> toInt16(x), hay), y), arrayMap(x -> toInt16(x), needles))) > 0)
    AND hasAny(arrayMap(x -> toInt64(x), hay), arrayMap(x -> toInt64(x), needles))
        = (length(arrayFilter(y -> has(arrayMap(x -> toInt64(x), hay), y), arrayMap(x -> toInt64(x), needles))) > 0)
    AS has_any_agrees
FROM shapes;

-- Nullable arrays are handled by the scalar path on both architectures. A NULL
-- needle is satisfied only when the haystack also holds a NULL.
SELECT
    hasAll([1, 2, NULL, 4] :: Array(Nullable(Int32)), [NULL] :: Array(Nullable(Int32))) = 1,
    hasAll([1, 2, 3, 4] :: Array(Nullable(Int32)), [NULL] :: Array(Nullable(Int32))) = 0,
    hasAll([1, NULL, 3] :: Array(Nullable(Int64)), [1, 3] :: Array(Nullable(Int64))) = 1,
    hasAll([1, NULL, 3] :: Array(Nullable(Int64)), [2] :: Array(Nullable(Int64))) = 0,
    hasAny([1, 2, NULL] :: Array(Nullable(Int16)), [NULL] :: Array(Nullable(Int16))) = 1,
    hasAny([1, 2, 3] :: Array(Nullable(Int16)), [NULL] :: Array(Nullable(Int16))) = 0;

-- Long Nullable arrays: past the kernel threshold, so this is the case that
-- proves nulls are still diverted before any vector load. With no NULL among
-- the needles, the NULL slots of the haystack cannot satisfy anything, so the
-- answer must match the same call on the haystack with those slots removed.
-- The compacted side is short enough to run the scalar path, so the two sides
-- exercise different code and agreement is meaningful.
SELECT DISTINCT
    hasAll(nullable_hay, needles)
        = hasAll(arrayFilter(x -> x IS NOT NULL, nullable_hay), needles) AS nullable_long_agrees
FROM (
    SELECT
        arrayMap(x -> if(x % 7 = 0, NULL, toInt32(x)), range(n)) AS nullable_hay,
        arrayJoin([
            arrayMap(x -> toInt32(x) :: Nullable(Int32), [1, 2, 3]),
            arrayMap(x -> toInt32(x) :: Nullable(Int32), [8, 9]),
            arrayMap(x -> toInt32(x) :: Nullable(Int32), [1, 30]),
            arrayMap(x -> toInt32(x) :: Nullable(Int32), [9999])
        ]) AS needles
    FROM (SELECT arrayJoin([33, 64, 65, 200]) AS n)
);

-- Boundary values: a kernel that mishandles the sign bit or the type limits
-- fails here while passing on small positive values.
SELECT
    hasAll([-128, 0, 127] :: Array(Int8), [-128, 127] :: Array(Int8)) = 1,
    hasAll([-32768, 0, 32767] :: Array(Int16), [-32768, 32767] :: Array(Int16)) = 1,
    hasAll([-2147483648, 0, 2147483647] :: Array(Int32), [-2147483648, 2147483647] :: Array(Int32)) = 1,
    hasAll([-9223372036854775808, 0, 9223372036854775807] :: Array(Int64), [-9223372036854775808, 9223372036854775807] :: Array(Int64)) = 1,
    hasAll([255, 0] :: Array(UInt8), [255] :: Array(UInt8)) = 1,
    hasAll([18446744073709551615, 0] :: Array(UInt64), [18446744073709551615] :: Array(UInt64)) = 1,
    hasAll([-1, 1] :: Array(Int8), [-1] :: Array(Int8)) = 1,
    hasAll([1, 2] :: Array(Int8), [-1] :: Array(Int8)) = 0;

-- Long arrays built entirely from boundary values, so they run in the kernel
-- rather than the short-input scalar path.
SELECT DISTINCT
    hasAll(hay, needles) = (length(arrayFilter(y -> NOT has(hay, y), needles)) = 0) AS extremes_long_agree
FROM (
    SELECT
        arrayMap(x -> if(x % 3 = 0, toInt64(-9223372036854775808), if(x % 3 = 1, toInt64(9223372036854775807), toInt64(0))), range(100)) AS hay,
        arrayJoin([
            [toInt64(-9223372036854775808)],
            [toInt64(9223372036854775807)],
            [toInt64(0)],
            [toInt64(-9223372036854775808), toInt64(9223372036854775807), toInt64(0)],
            [toInt64(1)]
        ]) AS needles
);

-- Duplicates on either side must not change the answer.
SELECT DISTINCT
    hasAll(hay, needles) = (length(arrayFilter(y -> NOT has(hay, y), needles)) = 0) AS duplicates_agree
FROM (
    SELECT
        arrayJoin([
            arrayMap(x -> toInt16(5), range(100)),
            arrayMap(x -> toInt16(x % 3), range(100))
        ]) AS hay,
        arrayJoin([
            arrayMap(x -> toInt16(5), range(40)),
            [toInt16(5)],
            [toInt16(6)],
            arrayMap(x -> toInt16(x % 3), range(40))
        ]) AS needles
);

-- Types whose physical column is an integer vector reach the same kernel.
SELECT
    hasAll(arrayMap(x -> toDate(x), range(100)), [toDate(50)]) = 1,
    hasAll(arrayMap(x -> toDate(x), range(100)), [toDate(200)]) = 0,
    hasAll(arrayMap(x -> toDateTime(x, 'UTC'), range(100)), [toDateTime(50, 'UTC')]) = 1,
    hasAll(arrayMap(x -> toDate32(x), range(100)), [toDate32(50)]) = 1,
    hasAll(arrayMap(x -> toBool(x % 2), range(100)), [true, false]) = 1;

-- Mixed integer widths: both arguments are cast to the common supertype before
-- the kernel is chosen, so the call must still agree with the reference.
SELECT
    hasAll(arrayMap(x -> toInt32(x), range(100)), arrayMap(x -> toInt64(x), [1, 2, 3])) = 1,
    hasAll(arrayMap(x -> toInt32(x), range(100)), arrayMap(x -> toInt64(x), [1000])) = 0,
    hasAll(arrayMap(x -> toUInt8(x), range(100)), arrayMap(x -> toUInt64(x), [1, 99])) = 1,
    hasAll(arrayMap(x -> toInt16(x), range(100)), arrayMap(x -> toInt8(x), [1, 99])) = 1;

-- Types the kernel does not claim keep working through the generic path.
SELECT
    hasAll(arrayMap(x -> toFloat64(x), range(100)), [toFloat64(50)]) = 1,
    hasAll(arrayMap(x -> toFloat64(x), range(100)), [toFloat64(200)]) = 0,
    hasAll(arrayMap(x -> toString(x), range(100)), ['50']) = 1,
    hasAll(arrayMap(x -> toDecimal64(x, 2), range(100)), [toDecimal64(50, 2)]) = 1,
    hasAll(arrayMap(x -> toIPv4('1.2.3.' || toString(x % 256)), range(100)), [toIPv4('1.2.3.50')]) = 1,
    hasAll([[1], [2], [3]] :: Array(Array(Int32)), [[2]] :: Array(Array(Int32))) = 1;

-- LowCardinality wrappers.
SELECT
    hasAll(arrayMap(x -> toLowCardinality(toInt32(x)), range(100)), [toLowCardinality(toInt32(50))]) = 1,
    hasAll(arrayMap(x -> toLowCardinality(toInt32(x)), range(100)), [toLowCardinality(toInt32(200))]) = 0;

-- A table-driven pass, so the kernel also sees non-constant columns arriving
-- one block at a time rather than a single literal array.
SELECT DISTINCT
    hasAll(hay, needles) = (length(arrayFilter(y -> NOT has(hay, y), needles)) = 0) AS table_driven_agrees
FROM (
    SELECT
        groupArray(toInt16(n % 251)) AS hay,
        arrayReverse(groupArray(toInt16(n % 251))) AS needles
    FROM t_has_all
    GROUP BY id % 4
);

DROP TABLE t_has_all;
