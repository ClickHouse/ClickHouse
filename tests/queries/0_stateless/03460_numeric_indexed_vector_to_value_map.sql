select 'TEST numericIndexedVectorToMap';

DROP TABLE IF EXISTS uin_value_details_int32_int8;
CREATE TABLE uin_value_details_int32_int8
(
    ds Date,
    uin UInt32,
    value Int8
)
ENGINE = MergeTree()
ORDER BY ds;
INSERT INTO uin_value_details_int32_int8 (ds, uin, value) values ('2023-12-26', 105, -5), ('2023-12-26', 104, -128), ('2023-12-26', 103, 3), ('2023-12-26', 102, -2), ('2023-12-26', 10000001, -127), ('2023-12-26', 10000002, 127), ('2023-12-26', 10000003, 25), ('2023-12-26', 10000004, 38);
INSERT INTO uin_value_details_int32_int8 (ds, uin, value) values ('2023-12-27', 103, -4), ('2023-12-27', 104, -5), ('2023-12-27', 105, -5), ('2023-12-27', 106, 6), ('2023-12-27', 10000001, 7), ('2023-12-27', 10000002, 3);

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details_int32_int8 
) as vec_1,
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-27')
    from uin_value_details_int32_int8 
) as vec_2
select arrayJoin([
    numericIndexedVectorToMap(vec_1)
    , numericIndexedVectorToMap(vec_2)
]);

DROP TABLE IF EXISTS uin_value_details_int32_int64;
CREATE TABLE uin_value_details_int32_int64
(
    ds Date,
    uin UInt32,
    value Int64
)
ENGINE = MergeTree()
ORDER BY ds;
INSERT INTO uin_value_details_int32_int64 (ds, uin, value) values ('2023-12-26', 105, -9223372036854775808), ('2023-12-26', 104, 9223372036854775807), ('2023-12-26', 103, 3), ('2023-12-26', 102, -2), ('2023-12-26', 10000001, -127), ('2023-12-26', 10000002, 127), ('2023-12-26', 10000003, 25), ('2023-12-26', 10000004, 38);
INSERT INTO uin_value_details_int32_int64 (ds, uin, value) values ('2023-12-27', 103, -9223372036854775807), ('2023-12-27', 104, -9223372036854775806), ('2023-12-27', 105, -5), ('2023-12-27', 106, 6), ('2023-12-27', 10000001, 7), ('2023-12-27', 10000002, 3);

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details_int32_int64
) as vec_1,
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-27')
    from uin_value_details_int32_int64
) as vec_2
select arrayJoin([
    numericIndexedVectorToMap(vec_1)
    , numericIndexedVectorToMap(vec_2)
]);

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
]);
