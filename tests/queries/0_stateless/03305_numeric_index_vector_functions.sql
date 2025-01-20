DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    metric_id UInt32,
    uin UInt32,
    value UInt64
)
ENGINE = MergeTree()
ORDER BY ds;

INSERT INTO uin_value_details (ds, metric_id, uin, value) values ('2023-12-26', 500001, 105, 5), ('2023-12-26', 500001, 104, 4), ('2023-12-26', 500001, 103, 3), ('2023-12-26', 500001, 102, 2), ('2023-12-26', 500001, 10000001, 1);
INSERT INTO uin_value_details (ds, metric_id, uin, value) values ('2023-12-26', 500002, 103, 4), ('2023-12-26', 500002, 104, 5), ('2023-12-26', 500002, 105, 5), ('2023-12-26', 500002, 106, 6), ('2023-12-26', 500002, 1000000007, 7);
INSERT INTO uin_value_details (ds, metric_id, uin, value) values ('2023-12-26', 500003, 101, 2), ('2023-12-26', 500003, 103, 3), ('2023-12-26', 500003, 108, 8), ('2023-12-26', 500003, 109, 9), ('2023-12-26', 500003, 110, 10);
INSERT INTO uin_value_details (ds, metric_id, uin, value) values ('2023-12-26', 500004, 1000001, 1), ('2023-12-26', 500004, 103, 1), ('2023-12-26', 500004, 108, 8), ('2023-12-26', 500004, 109, 3), ('2023-12-26', 500004, 110, 2);

select numericIndexedVectorShortDebugString(groupNumericIndexedVectorState(uin, value)) from uin_value_details;
select groupNumericIndexedVector(uin, value) from uin_value_details;
select numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(uin, value)) from uin_value_details;
select numericIndexedVectorCardinality(groupNumericIndexedVectorState(uin, value)) from uin_value_details;

select 'TEST groupNumericIndexedVector';
with
(
    select groupNumericIndexedVectorStateIf(uin, value, metric_id = 500001)
    from uin_value_details
) as vec_1,
(
    select groupNumericIndexedVectorStateIf(uin, value, metric_id = 500002)
    from uin_value_details
) as vec_2,
(
    select groupNumericIndexedVectorStateIf(uin, value, metric_id = 500003)
    from uin_value_details
) as vec_3
,(
    select groupNumericIndexedVectorStateIf(uin, value, metric_id = 500004)
    from uin_value_details
) as vec_4
select arrayJoin([
    numericIndexedVectorToMap(vec_1)
    , numericIndexedVectorToMap(vec_2)
    , numericIndexedVectorToMap(vec_3)
    , numericIndexedVectorToMap(vec_4)
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, toUInt64(2)))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, vec_2))
])
