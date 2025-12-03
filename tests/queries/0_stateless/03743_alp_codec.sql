DROP TABLE IF EXISTS base;
DROP TABLE IF EXISTS alp;

CREATE TABLE base
(
    id   UInt32,
    f32  Float32,
    f64  Float64
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE alp
(
    id   UInt32,
    f32  Float32 CODEC(ALP),
    f64  Float64 CODEC(ALP)
)
ENGINE = MergeTree
ORDER BY id;

-- 100k rows of deterministic "trend + seasonality + spikes" data
INSERT INTO base
SELECT
    number AS id,
    -- Float: pseudo "CPU usage" in percent
    --  - baseline ~50%
    --  - slow upward trend
    --  - short cycle (like daily pattern)
    --  - occasional spikes
    toFloat32(
            (
                5000                                        -- 50.00%
                + number / 10                               -- slow trend: +0.1 per step in integer cents
                + ((number % 1000) - 500) / 5               -- cycle: +/- 100 -> +/-1.00%
                + if(number % 10000 = 0, 2000, 0)           -- rare spikes: +20.00%
            ) / 100.0                                       -- convert "cents" to float
    ) AS f32,
    toFloat64(
            (
                5000                                        -- 50.00%
                + number / 10                               -- slow trend: +0.1 per step in integer cents
                + ((number % 1000) - 500) / 5               -- cycle: +/- 100 -> +/-1.00%
                + if(number % 10000 = 0, 2000, 0)           -- rare spikes: +20.00%
            ) / 100.0                                       -- convert "cents" to float
    ) AS f64
FROM numbers(100000);

INSERT INTO alp
SELECT id, f32, f64
FROM base;

OPTIMIZE TABLE base FINAL;
OPTIMIZE TABLE alp FINAL;

SELECT SUM(a.f32 <> b.f32) AS f32_errors,
       SUM(a.f64 <> b.f64) AS f64_errors
FROM base AS b
INNER JOIN alp AS a USING id;
