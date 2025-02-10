-- Basic Aggregate Functions
select 'TEST groupNumericIndexedVector';

DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value UInt64
)
ENGINE = MergeTree()
ORDER BY ds;

INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-26', 105, 5), ('2023-12-26', 104, 4), ('2023-12-26', 103, 3);
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-27', 10000001, 7), ('2023-12-27', 10000002, 3);

select numericIndexedVectorShortDebugString(groupNumericIndexedVectorState(uin, value)) from uin_value_details;
select groupNumericIndexedVector(uin, value) from uin_value_details;
select numericIndexedVectorAllValueSum(groupNumericIndexedVectorState(uin, value)) from uin_value_details;
select numericIndexedVectorCardinality(groupNumericIndexedVectorState(uin, value)) from uin_value_details;

select numericIndexedVectorShortDebugString(groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')) from uin_value_details;
select numericIndexedVectorShortDebugString(groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-27')) from uin_value_details;


-- TEST numericIndexedVectorPointwise operations: Add Subtract Multiply Divide Equal NotEqual Less LessEqual Greater GreaterEqual
select 'TEST numericIndexedVectorPointwise operations: Add Subtract Multiply Divide Equal NotEqual Less LessEqual Greater GreaterEqual';

DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value UInt64
)
ENGINE = MergeTree()
ORDER BY ds;
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-26', 105, 5), ('2023-12-26', 104, 4), ('2023-12-26', 103, 3), ('2023-12-26', 102, 2), ('2023-12-26', 10000001, 1), ('2023-12-26', 10000002, 2), ('2023-12-26', 10000003, 2), ('2023-12-26', 10000004, 2);
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-27', 103, 4), ('2023-12-27', 104, 5), ('2023-12-27', 105, 5), ('2023-12-27', 106, 6), ('2023-12-27', 10000001, 7), ('2023-12-27', 10000002, 3);

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
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, 2))
]);

-- TEST numericIndexedVectorPointwise operations with Float64 values
select 'TEST numericIndexedVectorPointwise operations with Float64 values';

DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value Float64
)
ENGINE = MergeTree()
ORDER BY ds;
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-26', 105, 5), ('2023-12-26', 104, 4), ('2023-12-26', 103, 3), ('2023-12-26', 102, 2), ('2023-12-26', 10000001, 1), ('2023-12-26', 10000002, 2), ('2023-12-26', 10000003, 2), ('2023-12-26', 10000004, 2);
INSERT INTO uin_value_details (ds, uin, value) values ('2023-12-27', 103, 4), ('2023-12-27', 104, 5), ('2023-12-27', 105, 5), ('2023-12-27', 106, 6), ('2023-12-27', 10000001, 7), ('2023-12-27', 10000002, 3);

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
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, 2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, vec_2))
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, 2))
]);

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
