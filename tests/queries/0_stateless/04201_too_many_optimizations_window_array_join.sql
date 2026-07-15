-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101693.
-- The combination of `ARRAY JOIN`, window functions and a filter that references
-- window-output columns used to cause `removeUnusedColumns` and `mergeExpressions`
-- to oscillate forever, eventually exceeding the optimization limit and throwing
-- TOO_MANY_QUERY_PLAN_OPTIMIZATIONS.

DROP TABLE IF EXISTS test_repro_101693;

CREATE TABLE test_repro_101693
(
    dt     Date,
    sym    String,
    market Int32,
    px     Float64,
    qty1   Float64,
    qty2   Float64,
    ts     Int64
) ENGINE = MergeTree() ORDER BY (sym, ts);

INSERT INTO test_repro_101693
SELECT
    toDate('2025-03-13')                                       AS dt,
    concat('B', toString(c))                                   AS sym,
    1                                                          AS market,
    round(20.0 + (number % 7) * 0.2 - (number % 3) * 0.1, 2)   AS px,
    qty1 % 500 + 100                                           AS qty1,
    qty2 % 800 + 200                                           AS qty2,
    100000 + number * 3                                        AS ts
FROM
(
    -- Deterministic pseudo-random values. The original repro used
    -- `generateRandom(...) LIMIT 20` with `rowNumberInAllBlocks`, but `generateRandom` fills the
    -- columns of each block sequentially from one RNG stream, so the generated values silently
    -- depended on how the LIMIT was chunked into blocks by the plan.
    SELECT
        number,
        toInt64(intHash64(number)) % 1000 AS qty1,
        toInt64(intHash64(number + 100)) % 1000 AS qty2
    FROM numbers(20)
) AS t
CROSS JOIN (SELECT number AS c FROM numbers(3)) AS syms
ORDER BY sym, ts;

-- Original query + ORDER BY ALL to make result deterministic

WITH
    base AS
    (
        SELECT
            sym,
            px,
            qty1,
            qty2,
            ts,
            2 AS N
        FROM test_repro_101693
        WHERE dt = '2025-03-13'
          AND ts >= 100000
          AND market = 1
        WINDOW w0 AS (PARTITION BY sym ORDER BY ts)
    ),
    expanded AS
    (
        SELECT *,
            i,
            arrayReverse(groupArray(px)   OVER w)[i]     AS prev_px1,
            arrayReverse(groupArray(px)   OVER w)[i + 1] AS prev_px2,
            arrayReverse(groupArray(ts)   OVER w)[i]     AS prev_ts1,
            arrayReverse(groupArray(ts)   OVER w)[i + 1] AS prev_ts2,
            arrayReverse(groupArray(qty2) OVER w)[i + 1] AS prev_qty2_1,
            arrayReverse(groupArray(qty2) OVER w)[i]     AS prev_qty2_2
        FROM base
        ARRAY JOIN range(1, 100) AS i
        WHERE i < N
        WINDOW w AS
        (
            PARTITION BY (sym, i)
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    ),
    result AS
    (
        SELECT
            sym,
            i,
            px,
            if(any(px) = any(prev_px2),
               least(max(qty2) - any(prev_qty2_1), max(qty1)),
               max(qty2)
            ) AS final_qty
        FROM expanded
        WHERE px >= least(prev_px1, prev_px2)
        GROUP BY sym, i, px
    )
SELECT * FROM result
SETTINGS allow_experimental_analyzer = 1
FORMAT Null;

-- Original query + ORDER BY ALL to make result deterministic
WITH
    base AS
    (
        SELECT
            sym,
            px,
            qty1,
            qty2,
            ts,
            2 AS N
        FROM test_repro_101693
        WHERE dt = '2025-03-13'
          AND ts >= 100000
          AND market = 1
        WINDOW w0 AS (PARTITION BY sym ORDER BY ts)
    ),
    expanded AS
    (
        SELECT *,
            i,
            arrayReverse(groupArray(px)   OVER w)[i]     AS prev_px1,
            arrayReverse(groupArray(px)   OVER w)[i + 1] AS prev_px2,
            arrayReverse(groupArray(ts)   OVER w)[i]     AS prev_ts1,
            arrayReverse(groupArray(ts)   OVER w)[i + 1] AS prev_ts2,
            arrayReverse(groupArray(qty2) OVER w)[i + 1] AS prev_qty2_1,
            arrayReverse(groupArray(qty2) OVER w)[i]     AS prev_qty2_2
        FROM base
        ARRAY JOIN range(1, 100) AS i
        WHERE i < N
        WINDOW w AS
        (
            PARTITION BY (sym, i)
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    ),
    result AS
    (
        SELECT
            sym,
            i,
            px,
            if(any(px) = any(prev_px2),
               least(max(qty2) - any(prev_qty2_1), max(qty1)),
               max(qty2)
            ) AS final_qty
        FROM expanded
        WHERE px >= least(prev_px1, prev_px2)
        GROUP BY sym, i, px
    )
SELECT * FROM result
ORDER BY ALL
SETTINGS allow_experimental_analyzer = 1;

DROP TABLE test_repro_101693;