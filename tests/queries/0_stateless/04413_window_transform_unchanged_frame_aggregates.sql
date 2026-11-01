-- `WindowTransform` reuses the previous row's result when the frame is unchanged instead of
-- re-finalizing. Exercise it with variable-length and compacting finalizers (`groupArray`, `sumMap`,
-- `quantilesExact`, `groupArraySorted`, `uniqExact`). The aggregates are order-independent (or sorted),
-- so the output is deterministic regardless of how tied rows happen to be ordered. A small
-- `max_block_size` makes peer groups cross block boundaries (the shortcut re-finalizes at each block
-- start), and a separate run keeps each peer group inside one block.

-- Peer-group frame (RANGE CURRENT ROW AND CURRENT ROW), peer groups crossing blocks.
SELECT DISTINCT k,
    arraySort(groupArray(number) OVER w),
    sumMap([number % 3], [number]) OVER w,
    quantilesExact(0.5, 0.9)(number) OVER w,
    groupArraySorted(3)(number) OVER w,
    uniqExact(number % 3) OVER w
FROM (SELECT number, intDiv(number, 4) AS k FROM numbers(13))
WINDOW w AS (ORDER BY k RANGE BETWEEN CURRENT ROW AND CURRENT ROW)
ORDER BY k
SETTINGS max_block_size = 3;

-- Same, peer groups within a single block.
SELECT DISTINCT k,
    arraySort(groupArray(number) OVER w),
    sumMap([number % 3], [number]) OVER w,
    quantilesExact(0.5, 0.9)(number) OVER w,
    groupArraySorted(3)(number) OVER w
FROM (SELECT number, intDiv(number, 4) AS k FROM numbers(13))
WINDOW w AS (ORDER BY k RANGE BETWEEN CURRENT ROW AND CURRENT ROW)
ORDER BY k;

-- Whole-partition frame crossing blocks.
SELECT DISTINCT k,
    arraySort(groupArray(number) OVER w2),
    quantilesExact(0.5)(number) OVER w2
FROM (SELECT number, intDiv(number, 4) AS k FROM numbers(13))
WINDOW w2 AS (PARTITION BY k ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY k
SETTINGS max_block_size = 2;
