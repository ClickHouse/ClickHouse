DROP TABLE IF EXISTS alp_vs_gorilla_base;
DROP TABLE IF EXISTS alp_vs_gorilla_alp;
DROP TABLE IF EXISTS alp_vs_gorilla_gorilla;

CREATE TABLE alp_vs_gorilla_base
(
    id   UInt32,
    f32  Float32,
    f64  Float64
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE alp_vs_gorilla_alp
(
    id   UInt32,
    f32  Float32 CODEC(ALP),
    f64  Float64 CODEC(ALP)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE alp_vs_gorilla_gorilla
(
    id   UInt32,
    f32  Float32 CODEC(Gorilla),
    f64  Float64 CODEC(Gorilla)
)
ENGINE = MergeTree
ORDER BY id;

-- 1M rows of deterministic "trend + seasonality + spikes" data
INSERT INTO alp_vs_gorilla_base
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
FROM numbers(1000000);

INSERT INTO alp_vs_gorilla_alp
SELECT id, f32, f64
FROM alp_vs_gorilla_base;

INSERT INTO alp_vs_gorilla_gorilla
SELECT id, f32, f64
FROM alp_vs_gorilla_base;

OPTIMIZE TABLE alp_vs_gorilla_base FINAL;
OPTIMIZE TABLE alp_vs_gorilla_alp FINAL;
OPTIMIZE TABLE alp_vs_gorilla_gorilla FINAL;

SELECT SUM(a.f32 <> b.f32) AS f32_errors,
       SUM(a.f64 <> b.f64) AS f64_errors
FROM alp_vs_gorilla_base AS b
INNER JOIN alp_vs_gorilla_alp AS a USING id;

SELECT
    table,
    column,
    sum(data_compressed_bytes) AS compressed,
    sum(data_uncompressed_bytes) AS uncompressed,
    round(compressed / uncompressed * 100, 2) AS ratio_percent
FROM system.parts_columns
WHERE table LIKE 'alp_vs_gorilla_%'
  AND active
  AND column LIKE 'f%'
GROUP BY table, column
ORDER BY ratio_percent;
