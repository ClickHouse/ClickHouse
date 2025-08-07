select 'TEST numericIndexedVectorPointwise operations with zero values and Int16 value type';
DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value Int16
)
ENGINE = MergeTree()
ORDER BY ds;
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-26', 10000001, 7), ('2023-12-26', 10000002, 8), ('2023-12-26', 10000003, 0), ('2023-12-26', 10000004, 0), ('2023-12-26', 20000005, 0), ('2023-12-26', 30000005, 100), ('2023-12-26', 50000005, 0);
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-27', 10000001, 7), ('2023-12-27', 10000002, -8), ('2023-12-27', 10000003, 30), ('2023-12-27', 10000004, -3), ('2023-12-27', 20000005, 0), ('2023-12-27', 40000005, 100), ('2023-12-27', 60000005, 0);

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
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, -7))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, -8))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_2, -5))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_2, 0))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, 2))
]);

DROP TABLE IF EXISTS uin_value_details;
