-- Tags: no-parallel

-- test toVectorCompactArray; toVectorCompactBitset; toVectorCompactBitsetDense
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

insert into uin_value_details (ds, uin, value) select '2023-12-26', number, number * number from numbers(1000);
insert into uin_value_details (ds, uin, value) select '2023-12-27', number, number from numbers(1000);

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
    numericIndexedVectorAllValueSum(numericIndexedVectorPointwiseDivide(vec_1, vec_2))
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

insert into uin_value_details (ds, uin, value) select '2023-12-26', number, number * number from numbers(5000);
insert into uin_value_details (ds, uin, value) select '2023-12-27', number, number from numbers(5000);

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
    numericIndexedVectorAllValueSum(numericIndexedVectorPointwiseDivide(vec_1, vec_2))
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

insert into uin_value_details (ds, uin, value) select '2023-12-26', number, number * number from numbers(100000);
insert into uin_value_details (ds, uin, value) select '2023-12-27', number, number from numbers(100000);
insert into uin_value_details (ds, uin, value) select '2023-12-28', number * 3, number * 3 * number * 3 from numbers(100000);
insert into uin_value_details (ds, uin, value) select '2023-12-29', number * 3, number * 3 from numbers(100000);

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
select arrayJoin([
    numericIndexedVectorAllValueSum(numericIndexedVectorPointwiseDivide(vec_1, vec_2))
    , numericIndexedVectorAllValueSum(numericIndexedVectorPointwiseDivide(vec_3, vec_4))
    , numericIndexedVectorAllValueSum(numericIndexedVectorPointwiseDivide(vec_1, vec_3))
]);


-- test insert empty aggregate function
DROP TABLE IF EXISTS numeric_indexed_vector;
CREATE TABLE numeric_indexed_vector
(
    ds Date,
    vector AggregateFunction(groupNumericIndexedVector, UInt32, Float64)
)
ENGINE = MergeTree()
ORDER BY ds;

insert into numeric_indexed_vector (ds) values ('2023-12-26');
