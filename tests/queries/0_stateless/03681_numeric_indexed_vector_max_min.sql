select 'TEST numericIndexedVectorPointwiseMax/Min & GetMax/Min with zero/negatives and Float64 value type';

DROP TABLE IF EXISTS uin_value_details_u32_f64;

CREATE TABLE uin_value_details_u32_f64
(
    ds Date,
    uin UInt32,
    value Float64
)
ENGINE = MergeTree()
ORDER BY ds;

INSERT INTO uin_value_details_u32_f64 (ds, uin, value) VALUES
('2025-10-20', 10000001, 7.3),
('2025-10-20', 10000002, 8.3),
('2025-10-20', 10000003, 0),
('2025-10-20', 10000004, 0),
('2025-10-20', 20000005, 0),
('2025-10-20', 30000005, 100.6543782),
('2025-10-20', 50000005, 0);

INSERT INTO uin_value_details_u32_f64 (ds, uin, value) VALUES
('2025-10-21', 10000001, 7.3),
('2025-10-21', 10000002, -8.3),
('2025-10-21', 10000003, 30.5),
('2025-10-21', 10000004, -3.384),
('2025-10-21', 20000005, 0),
('2025-10-21', 40000005, 100.66666666),
('2025-10-21', 60000005, 0);

WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-20')
    FROM uin_value_details_u32_f64 
) AS vec_1,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_f64 
) AS vec_2
SELECT arrayJoin([
    numericIndexedVectorToMap(vec_1)
    , numericIndexedVectorToMap(vec_2)
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(vec_1, vec_2))
]);

WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-20')
    FROM uin_value_details_u32_f64
) AS vec_1a,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_f64
) AS vec_2a
SELECT arrayJoin([
    numericIndexedVectorGetMaxValue(vec_1a) AS max_vec1
    , numericIndexedVectorGetMinValue(vec_1a) AS min_vec1
    , numericIndexedVectorGetMaxValue(vec_2a) AS max_vec2
    , numericIndexedVectorGetMinValue(vec_2a) AS min_vec2
]);

DROP TABLE IF EXISTS uin_value_details_u32_f64;

SELECT 'TEST numericIndexedVectorPointwise Max/Min & GetMax/Min with Int32 value type';

DROP TABLE IF EXISTS uin_value_details_u32_i32;

CREATE TABLE uin_value_details_u32_i32
(
    ds Date,
    uin UInt32,
    value Int32
)
ENGINE = MergeTree()
ORDER BY ds;

INSERT INTO uin_value_details_u32_i32 (ds, uin, value) VALUES
('2025-10-20', 10000001, 7),
('2025-10-20', 10000002, 8),
('2025-10-20', 10000003, 0),
('2025-10-20', 10000004, 0),
('2025-10-20', 20000005, 0),
('2025-10-20', 30000005, 101),
('2025-10-20', 50000005, 0);

INSERT INTO uin_value_details_u32_i32 (ds, uin, value) VALUES
('2025-10-21', 10000001, 7),
('2025-10-21', 10000002, -8),
('2025-10-21', 10000003, 31),
('2025-10-21', 10000004, -3),
('2025-10-21', 20000005, 0),
('2025-10-21', 40000005, 100),
('2025-10-21', 60000005, 0);

WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-20')
    FROM uin_value_details_u32_i32
) AS vec_1,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_i32
) AS vec_2
SELECT arrayJoin([
    numericIndexedVectorToMap(vec_1),
    numericIndexedVectorToMap(vec_2),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(vec_1, vec_2)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(vec_1, vec_2))
]);

WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-20')
    FROM uin_value_details_u32_i32
) AS vec_1a,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_i32
) AS vec_2a
SELECT arrayJoin([
    numericIndexedVectorGetMaxValue(vec_1a) AS max_vec1,
    numericIndexedVectorGetMinValue(vec_1a) AS min_vec1,
    numericIndexedVectorGetMaxValue(vec_2a) AS max_vec2,
    numericIndexedVectorGetMinValue(vec_2a) AS min_vec2
]);


DROP TABLE IF EXISTS uin_value_details_u32_i32;

SELECT 'TEST numericIndexedVectorPointwise Max/Min & GetMax/Min with UInt64 value type';

DROP TABLE IF EXISTS uin_value_details_u32_u64;

CREATE TABLE uin_value_details_u32_u64
(
    ds Date,
    uin UInt32,
    value UInt64
)
ENGINE = MergeTree()
ORDER BY ds;

INSERT INTO uin_value_details_u32_u64 (ds, uin, value) VALUES
('2025-10-20', 10000001, 7),
('2025-10-20', 10000002, 8),
('2025-10-20', 10000003, 0),
('2025-10-20', 10000004, 0),
('2025-10-20', 20000005, 0),
('2025-10-20', 30000005, 100),
('2025-10-20', 50000005, 0);

INSERT INTO uin_value_details_u32_u64 (ds, uin, value) VALUES
('2025-10-21', 10000001, 7),
('2025-10-21', 10000002, 83),
('2025-10-21', 10000003, 31),
('2025-10-21', 10000004, 3),
('2025-10-21', 20000005, 0),
('2025-10-21', 40000005, 101),
('2025-10-21', 60000005, 0);

WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-20')
    FROM uin_value_details_u32_u64
) AS vec_1,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_u64
) AS vec_2
SELECT arrayJoin([
    numericIndexedVectorToMap(vec_1),
    numericIndexedVectorToMap(vec_2),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(vec_1, vec_2)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(vec_1, vec_2))
]);

WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-20')
    FROM uin_value_details_u32_u64
) AS vec_1a,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_u64
) AS vec_2a
SELECT arrayJoin([
    numericIndexedVectorGetMaxValue(vec_1a) AS max_vec1,
    numericIndexedVectorGetMinValue(vec_1a) AS min_vec1,
    numericIndexedVectorGetMaxValue(vec_2a) AS max_vec2,
    numericIndexedVectorGetMinValue(vec_2a) AS min_vec2
]);

DROP TABLE IF EXISTS uin_value_details_u32_u64;
