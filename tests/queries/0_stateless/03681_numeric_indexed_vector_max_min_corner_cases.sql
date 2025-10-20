SELECT 'TEST numericIndexedVectorPointwiseMax/Min & GetMax/Min with Float64 value and empty/single-side/tie & errors';

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

-- Corner case: empty vector
WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = toDate('2000-01-01'))
    FROM uin_value_details_u32_f64 
) AS vec_empty,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_f64 
) AS vec_full
SELECT arrayJoin([
    numericIndexedVectorToMap(vec_empty),
    numericIndexedVectorToMap(vec_full),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(vec_empty, vec_full)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(vec_empty, vec_full))
]);

-- Corner case: equal vectors
WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_f64 
) AS v1,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_f64 
) AS v2
SELECT arrayJoin([
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v1, v2)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v1, v2))
]);

-- Corner case: unsupported scalar
WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_f64 
) AS v
SELECT numericIndexedVectorPointwiseMax(v, 1.0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Corner case: different index types
WITH
(
    SELECT groupNumericIndexedVectorStateIf(toUInt16(uin), value, ds = '2025-10-20')
    FROM uin_value_details_u32_f64 
) AS v_u64_index,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_f64 
) AS v_u32_index
SELECT numericIndexedVectorPointwiseMax(v_u64_index, v_u32_index); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


SELECT 'INT32 — boundary MIN/MAX, negatives/zeros, empty, errors';

DROP TABLE IF EXISTS uin_value_details_u32_i32;
CREATE TABLE uin_value_details_u32_i32 
(
    ds Date,
    uin UInt32,
    value Int32
)
ENGINE = MergeTree()
ORDER BY ds;

-- day1：include INT32_MIN/INT32_MAX
INSERT INTO uin_value_details_u32_i32 VALUES
('2025-10-20', 1, toInt32(-2147483648)),
('2025-10-20', 2, 0),
('2025-10-20', 3, -1),
('2025-10-20', 4, toInt32(2147483647));

INSERT INTO uin_value_details_u32_i32 VALUES
('2025-10-21', 1, toInt32(2147483647)),
('2025-10-21', 2, 5),
('2025-10-21', 3, 0),
('2025-10-21', 5, toInt32(-2147483648));

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
) AS v1,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_i32 
) AS v2
SELECT arrayJoin([
    numericIndexedVectorGetMaxValue(v1) AS max_vec1
    , numericIndexedVectorGetMinValue(v1) AS min_vec1
    , numericIndexedVectorGetMaxValue(v2) AS max_vec2
    , numericIndexedVectorGetMinValue(v2) AS min_vec2
]);

-- empty vector + non-empty vector
WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = toDate('2000-01-01'))
    FROM uin_value_details_u32_i32 
) AS empty_vec,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_i32 
) AS full_vec
SELECT arrayJoin([
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(empty_vec, full_vec)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(empty_vec, full_vec))
]);

DROP TABLE IF EXISTS uin_value_details_u32_i32;


SELECT 'UINT64 — boundary 0/MAX-1/MAX, empty, errors';

DROP TABLE IF EXISTS uin_value_details_u32_u64;
CREATE TABLE uin_value_details_u32_u64
(
    ds Date,
    uin UInt32,
    value UInt64
)
ENGINE = MergeTree()
ORDER BY ds;

INSERT INTO uin_value_details_u32_u64 VALUES
('2025-10-20', 1, toUInt64(0)),
('2025-10-20', 2, toUInt64(1)),
('2025-10-20', 3, toUInt64('1844674407370955161'));

INSERT INTO uin_value_details_u32_u64 VALUES
('2025-10-21', 1, toUInt64(2)),
('2025-10-21', 4, toUInt64('1844674407370955161')),
('2025-10-21', 5, toUInt64(0));

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
) AS v1,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, ds = '2025-10-21')
    FROM uin_value_details_u32_u64
) AS v2
SELECT arrayJoin([
    numericIndexedVectorGetMaxValue(v1) AS max_vec1
    , numericIndexedVectorGetMinValue(v1) AS min_vec1
    , numericIndexedVectorGetMaxValue(v2) AS max_vec2
    , numericIndexedVectorGetMinValue(v2) AS min_vec2
])
