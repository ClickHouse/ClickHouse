-- lastChangeAt: last change at ts=3, value stays 1 at ts=4,5 — result is 3, not 5
SELECT lastChangeAt(value, ts) FROM (SELECT number AS ts, [0, 0, 1, 1, 1][number] AS value FROM numbers(1, 5));

-- lastChangeAt: no change — returns first_ts
SELECT lastChangeAt(value, ts) FROM (SELECT number AS ts, 42 AS value FROM numbers(1, 3));

-- lastChangeAt: Decimal64 value type
SELECT lastChangeAt(toDecimal64(value, 1), ts) FROM (SELECT number AS ts, [0, 0, 1, 1, 1][number] AS value FROM numbers(1, 5));

-- lastChangeAt: DateTime64 timestamp — convert result back to milliseconds for timezone-stable output
SELECT toUnixTimestamp64Milli(lastChangeAt(value, toDateTime64(ts, 3, 'UTC'))) FROM (SELECT number AS ts, [0, 0, 1, 1, 1][number] AS value FROM numbers(1, 5));

-- lastChangeAt: Decimal128 value type
SELECT lastChangeAt(toDecimal128(value, 2), ts) FROM (SELECT number AS ts, [0, 0, 1, 1, 1][number] AS value FROM numbers(1, 5));

-- Merge: state1=(0,0,1,1 at ts=1..4) before state2=(1,2,2,2 at ts=5..8) → last change at ts=6
SELECT lastChangeAtMerge(state) FROM (
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT number AS ts, [0, 0, 1, 1][number] AS value FROM numbers(1, 4))
    UNION ALL
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT number AS ts, [1, 2, 2, 2][number - 4] AS value FROM numbers(5, 4))
);

-- Merge reversed UNION ALL order (tests b_before_a branch) → same result
SELECT lastChangeAtMerge(state) FROM (
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT number AS ts, [1, 2, 2, 2][number - 4] AS value FROM numbers(5, 4))
    UNION ALL
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT number AS ts, [0, 0, 1, 1][number] AS value FROM numbers(1, 4))
);

-- Merge: boundary change (state2 first_value=2 ≠ state1 last_value=1) → change at state2.first_ts=5
SELECT lastChangeAtMerge(state) FROM (
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT number AS ts, [0, 0, 1, 1][number] AS value FROM numbers(1, 4))
    UNION ALL
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT number AS ts, 2 AS value FROM numbers(5, 4))
);

-- Bitmap: change at ts=3 (bitmap {1,2,3} → {1,2,4}), stays {1,2,4} at ts=4 — result is 3, not 4
SELECT lastChangeAtMerge(state) FROM (
    SELECT lastChangeAtState(bmp, ts) AS state FROM (
        SELECT ts, groupBitmapState(toUInt32(n)) AS bmp
        FROM (SELECT 1 AS ts, arrayJoin([1, 2, 3]) AS n UNION ALL SELECT 2, arrayJoin([1, 2, 3]))
        GROUP BY ts ORDER BY ts
    )
    UNION ALL
    SELECT lastChangeAtState(bmp, ts) AS state FROM (
        SELECT ts, groupBitmapState(toUInt32(n)) AS bmp
        FROM (SELECT 3 AS ts, arrayJoin([1, 2, 4]) AS n UNION ALL SELECT 4, arrayJoin([1, 2, 4]))
        GROUP BY ts ORDER BY ts
    )
);

-- Bitmap: no change (all same bitmap) → returns first_ts=1
SELECT lastChangeAtMerge(state) FROM (
    SELECT lastChangeAtState(bmp, ts) AS state FROM (
        SELECT 1 AS ts, groupBitmapState(toUInt32(n)) AS bmp FROM (SELECT arrayJoin([1, 2, 3]) AS n)
    )
    UNION ALL
    SELECT lastChangeAtState(bmp, ts) AS state FROM (
        SELECT ts, groupBitmapState(toUInt32(n)) AS bmp
        FROM (SELECT 2 AS ts, arrayJoin([1, 2, 3]) AS n UNION ALL SELECT 3, arrayJoin([1, 2, 3]))
        GROUP BY ts ORDER BY ts
    )
);

-- Overlap merge order regression: (0@2),(1@3),(0@4) merged as [4]->[2]->[3] must give 4, not 2.
-- [4] merged with [2] produces an overlapping range [2,4] when [3] is merged next.
SELECT lastChangeAtMerge(state) FROM (
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT 4 AS ts, 0 AS value)
    UNION ALL
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT 2 AS ts, 0 AS value)
    UNION ALL
    SELECT lastChangeAtState(value, ts) AS state FROM (SELECT 3 AS ts, 1 AS value)
);

-- Error: String value type is unsupported
SELECT lastChangeAt('x', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
