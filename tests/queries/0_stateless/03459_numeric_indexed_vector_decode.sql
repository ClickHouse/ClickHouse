select 'TEST toVectorCompactArray';

DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value Float64
)
ENGINE = MergeTree()
ORDER BY ds;

insert into uin_value_details (ds, uin, value) select '2023-12-20', number, number * number from numbers(1000);
insert into uin_value_details (ds, uin, value) select '2023-12-21', number, number from numbers(1000);

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-20')
    from uin_value_details
) as vec_1,
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-21')
    from uin_value_details
) as vec_2
, numericIndexedVectorPointwiseDivide(vec_1, vec_2) as vec_3
select arrayJoin([
    numericIndexedVectorShortDebugString(vec_1)
    , toString(numericIndexedVectorAllValueSum(vec_1))
    , numericIndexedVectorShortDebugString(vec_2)
    , toString(numericIndexedVectorAllValueSum(vec_2))
    , numericIndexedVectorShortDebugString(vec_3)
    , toString(numericIndexedVectorAllValueSum(vec_3))
]);

select 'TEST toVectorCompactBitset';

DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value Float64
)
ENGINE = MergeTree()
ORDER BY ds;

insert into uin_value_details (ds, uin, value) select '2023-12-22', number, number * number from numbers(5000);
insert into uin_value_details (ds, uin, value) select '2023-12-23', number, number from numbers(5000);

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-22')
    from uin_value_details
) as vec_1,
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-23')
    from uin_value_details
) as vec_2
, numericIndexedVectorPointwiseDivide(vec_1, vec_2) as vec_3
select arrayJoin([
    numericIndexedVectorShortDebugString(vec_1)
    , toString(numericIndexedVectorAllValueSum(vec_1))
    , numericIndexedVectorShortDebugString(vec_2)
    , toString(numericIndexedVectorAllValueSum(vec_2))
    , numericIndexedVectorShortDebugString(vec_3)
    , toString(numericIndexedVectorAllValueSum(vec_3))
]);

select 'TEST toVectorCompactBitsetDense';

DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value Float64
)
ENGINE = MergeTree()
ORDER BY ds;

insert into uin_value_details (ds, uin, value) select '2023-12-26', number, number * number from numbers(30000);
insert into uin_value_details (ds, uin, value) select '2023-12-27', number, number from numbers(30000);
insert into uin_value_details (ds, uin, value) select '2023-12-28', number * 3, number * 3 * number * 3 from numbers(30000);
insert into uin_value_details (ds, uin, value) select '2023-12-29', number * 3, number * 3 from numbers(30000);

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-27')
    from uin_value_details
) as vec_2
, (
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-28')
    from uin_value_details
) as vec_3,
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-29')
    from uin_value_details
) as vec_4
, numericIndexedVectorPointwiseDivide(vec_1, vec_2) as vec_5
, numericIndexedVectorPointwiseDivide(vec_1, vec_3) as vec_6
, numericIndexedVectorPointwiseDivide(vec_1, vec_4) as vec_7
, numericIndexedVectorPointwiseDivide(vec_2, vec_3) as vec_8
, numericIndexedVectorPointwiseDivide(vec_2, vec_4) as vec_9
, numericIndexedVectorPointwiseDivide(vec_3, vec_4) as vec_10
select arrayJoin([
    numericIndexedVectorShortDebugString(vec_1)
    , toString(numericIndexedVectorAllValueSum(vec_1))
    , numericIndexedVectorShortDebugString(vec_2)
    , toString(numericIndexedVectorAllValueSum(vec_2))
    , numericIndexedVectorShortDebugString(vec_3)
    , toString(numericIndexedVectorAllValueSum(vec_3))
    , numericIndexedVectorShortDebugString(vec_4)
    , toString(numericIndexedVectorAllValueSum(vec_4))
    , numericIndexedVectorShortDebugString(vec_5)
    , toString(numericIndexedVectorAllValueSum(vec_5))
    , numericIndexedVectorShortDebugString(vec_6)
    , toString(numericIndexedVectorAllValueSum(vec_6))
    , numericIndexedVectorShortDebugString(vec_7)
    , toString(numericIndexedVectorAllValueSum(vec_7))
    , numericIndexedVectorShortDebugString(vec_8)
    , toString(numericIndexedVectorAllValueSum(vec_8))
    , numericIndexedVectorShortDebugString(vec_9)
    , toString(numericIndexedVectorAllValueSum(vec_9))
    , numericIndexedVectorShortDebugString(vec_10)
    , toString(numericIndexedVectorAllValueSum(vec_10))
]);


select 'test insert empty aggregate function';

DROP TABLE IF EXISTS numeric_indexed_vector;
CREATE TABLE numeric_indexed_vector
(
    ds Date,
    vector AggregateFunction(groupNumericIndexedVector, UInt32, Float64)
)
ENGINE = MergeTree()
ORDER BY ds;

insert into numeric_indexed_vector (ds) values ('2023-12-26');

DROP TABLE IF EXISTS uin_value_details;
DROP TABLE IF EXISTS uin_value_details;
DROP TABLE IF EXISTS uin_value_details;
DROP TABLE IF EXISTS numeric_indexed_vector;
