select 'TEST numericIndexedVectorPointwise operations with zero values and UInt16 index type';
DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt16,
    value Float64
)
ENGINE = MergeTree()
ORDER BY ds;
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-26', 1001, 7.3), ('2023-12-26', 1002, 8.3), ('2023-12-26', 1003, 0), ('2023-12-26', 1004, 0), ('2023-12-26', 2005, 0), ('2023-12-26', 3005, 100.6543782), ('2023-12-26', 5005, 0);
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-27', 1001, 7.3), ('2023-12-27', 1002, -8.3), ('2023-12-27', 1003, 30.5), ('2023-12-27', 1004, -3.384), ('2023-12-27', 2005, 0), ('2023-12-27', 4005, 100.66666666), ('2023-12-27', 6005, 0);

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-27')
    from uin_value_details
) as vec_2
select arrayJoin([
    numericIndexedVectorToMap(vec_1)
    , numericIndexedVectorToMap(vec_2)
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, -2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_2, 8))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_2, 0))
]);

DROP TABLE IF EXISTS uin_value_details;
