DROP TABLE IF EXISTS base;
DROP TABLE IF EXISTS alp;
DROP TABLE IF EXISTS gorilla;

CREATE TABLE base
(
    id   UInt32,
    f32  Float32,
    f64  Float64
)
ORDER BY id;

CREATE TABLE alp
(
    id   UInt32,
    f32  Float32 CODEC(ALP),
    f64  Float64 CODEC(ALP)
)
ORDER BY id;

CREATE TABLE gorilla
(
    id   UInt32,
    f32  Float32 CODEC(Gorilla),
    f64  Float64 CODEC(Gorilla)
)
ORDER BY id;

INSERT INTO base
SELECT
    number AS id,
    round(toFloat32(number / 20 + sin(number)), 2) + if(number % 10 = 0, number, 0) AS f32,
    round(toFloat64(number / 20 + sin(number)), 2) + if(number % 10 = 0, number, 0) AS f64
FROM numbers(10000);

INSERT INTO alp
SELECT id, f32, f64
FROM base;

INSERT INTO gorilla
SELECT id, f32, f64
FROM base;

OPTIMIZE TABLE base FINAL;
OPTIMIZE TABLE alp FINAL;
OPTIMIZE TABLE gorilla FINAL;

SELECT SUM(ABS(a.f32 - b.f32) > 0.0000001) AS f32_errors,
       SUM(ABS(a.f64 - b.f64) > 0.0000001) AS f64_errors
FROM base AS b
INNER JOIN alp AS a USING id;

SELECT
    table,
    column,
    sum(data_compressed_bytes) AS compressed,
    sum(data_uncompressed_bytes) AS uncompressed,
    round(compressed / uncompressed, 3) AS ratio
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table IN ('alp', 'gorilla')
  AND active
GROUP BY table, column
ORDER BY ratio, table, column;
