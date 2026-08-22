DROP TABLE IF EXISTS uin_value_details_int32_float64;
CREATE TABLE uin_value_details_int32_float64
(
    ds Date,
    uin UInt32,
    value Float64
)
ENGINE = MergeTree()
ORDER BY ds;
INSERT INTO uin_value_details_int32_float64 (ds, uin, value) values ('2023-12-26', 105, -549755813887.3421), ('2023-12-26', 104, 549755813877.3421), ('2023-12-26', 103, 3), ('2023-12-26', 102, -2), ('2023-12-26', 10000001, -127), ('2023-12-26', 10000002, 127), ('2023-12-26', 10000003, 25), ('2023-12-26', 10000004, 38);
INSERT INTO uin_value_details_int32_float64 (ds, uin, value) values ('2023-12-27', 103, -549755813887.34125083415), ('2023-12-27', 104, -9223372.32169785621), ('2023-12-27', 105, -5), ('2023-12-27', 106, 6), ('2023-12-27', 10000001, 7), ('2023-12-27', 10000002, 3);

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details_int32_float64
) as vec_1,
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-27')
    from uin_value_details_int32_float64
) as vec_2
select arrayJoin([
    numericIndexedVectorToMap(vec_1)
    , numericIndexedVectorToMap(vec_2)
    , numericIndexedVectorToMap(CAST(CAST(vec_1, 'String'), 'AggregateFunction(groupNumericIndexedVector, UInt32, Float64)'))
    , numericIndexedVectorToMap(CAST(CAST(vec_2, 'String'), 'AggregateFunction(groupNumericIndexedVector, UInt32, Float64)'))
]);

DROP TABLE IF EXISTS vector_int32_float64;
CREATE TABLE vector_int32_float64
(
    ds Date
    , vec AggregateFunction(groupNumericIndexedVector, UInt32, Float64)
    , vec_str String
)
ENGINE = MergeTree()
ORDER BY ds;

INSERT INTO vector_int32_float64 (ds, vec_str, vec) 
SELECT ds, groupNumericIndexedVectorState(uin, value) as vec,
       CAST(vec, 'String') as vec_str
FROM uin_value_details_int32_float64 group by ds;

select ds, numericIndexedVectorToMap(vec)
  , numericIndexedVectorToMap(CAST(vec_str, 'AggregateFunction(groupNumericIndexedVector, UInt32, Float64)'))
from vector_int32_float64;

DROP TABLE IF EXISTS uin_value_details_int32_float64;
DROP TABLE IF EXISTS vector_int32_float64;
