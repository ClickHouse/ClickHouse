DROP TABLE IF EXISTS array_distance_mixed_types;

CREATE TABLE array_distance_mixed_types
(
    id UInt64,
    u8 Array(UInt8),
    f32 Array(Float32),
    f64 Array(Float64)
)
ENGINE = Memory;

INSERT INTO array_distance_mixed_types VALUES
    (1, [1, 2, 3, 4], [1.5, 2.5, 3.5, 4.5], [1.25, 2.25, 3.25, 4.25]),
    (2, [5, 8, 13, 21], [4.0, 7.5, 12.5, 20.5], [4.25, 7.25, 12.25, 20.25]),
    (3, [34, 55, 89, 144], [33.5, 54.5, 88.5, 143.5], [33.25, 54.25, 88.25, 143.25]);

WITH
    CAST(u8, 'Array(Float32)') AS u8_f32,
    CAST(u8, 'Array(Float64)') AS u8_f64,
    CAST(f32, 'Array(Float64)') AS f32_f64
SELECT
    countIf(
        L1Distance(u8, f32) = L1Distance(u8_f32, f32)
        AND L2Distance(u8, f32) = L2Distance(u8_f32, f32)
        AND L2SquaredDistance(u8, f32) = L2SquaredDistance(u8_f32, f32)
        AND LinfDistance(u8, f32) = LinfDistance(u8_f32, f32)
        AND cosineDistance(u8, f32) = cosineDistance(u8_f32, f32)
    ) = count()
FROM array_distance_mixed_types;

WITH
    CAST(u8, 'Array(Float64)') AS u8_f64,
    CAST(f32, 'Array(Float64)') AS f32_f64
SELECT
    countIf(
        L1Distance(u8, f64) = L1Distance(u8_f64, f64)
        AND L2Distance(u8, f64) = L2Distance(u8_f64, f64)
        AND L2SquaredDistance(u8, f64) = L2SquaredDistance(u8_f64, f64)
        AND LinfDistance(u8, f64) = LinfDistance(u8_f64, f64)
        AND cosineDistance(u8, f64) = cosineDistance(u8_f64, f64)
        AND L1Distance(f32, f64) = L1Distance(f32_f64, f64)
        AND L2Distance(f32, f64) = L2Distance(f32_f64, f64)
        AND L2SquaredDistance(f32, f64) = L2SquaredDistance(f32_f64, f64)
        AND LinfDistance(f32, f64) = LinfDistance(f32_f64, f64)
        AND cosineDistance(f32, f64) = cosineDistance(f32_f64, f64)
    ) = count()
FROM array_distance_mixed_types;

SELECT
    countIf(
        L2Distance([toUInt8(1), 2, 3, 4], f64) = L2Distance(CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'), f64)
        AND L2Distance(f64, [toUInt8(1), 2, 3, 4]) = L2Distance(f64, CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'))
        AND cosineDistance([toUInt8(1), 2, 3, 4], f64) = cosineDistance(CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'), f64)
        AND cosineDistance(f64, [toUInt8(1), 2, 3, 4]) = cosineDistance(f64, CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'))
    ) = count()
FROM array_distance_mixed_types;

DROP TABLE array_distance_mixed_types;

DROP TABLE IF EXISTS array_distance_mixed_large;

CREATE TABLE array_distance_mixed_large
(
    id UInt64,
    u8 Array(UInt8),
    f64 Array(Float64)
)
ENGINE = Memory;

INSERT INTO array_distance_mixed_large
SELECT
    number,
    arrayMap(i -> toUInt8(i % 256), range(262144)),
    arrayMap(i -> toFloat64(i % 257), range(262144))
FROM numbers(4);

SET max_memory_usage = '6Mi';

SELECT sum(if(isFinite(L2Distance(u8, f64)), 1, 0)) = 4
FROM array_distance_mixed_large;

DROP TABLE array_distance_mixed_large;
