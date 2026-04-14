select 'TEST numericIndexedVectorPointwise operations with zero values and Int32 value type';
DROP TABLE IF EXISTS uin_value_details;
CREATE TABLE uin_value_details
(
    ds Date,
    uin UInt32,
    value Int32
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

select 'TEST numericIndexedVectorPointwise operations with Bitmap second argument';

-- Positive: pointwiseMultiply with a Bitmap keeps only indices present in the bitmap (bitmap treated as a vector of ones).
with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26' and uin in (10000001, 10000002, 30000005)
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, bm));

-- Negative: Bitmap element type must match the vector index type.
with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(toUInt64(uin)) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Negative: pointwise operations other than Multiply reject a Bitmap second argument.
with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

with
(
    select groupNumericIndexedVectorStateIf(uin, value, ds = '2023-12-26')
    from uin_value_details
) as vec_1,
(
    select groupBitmapState(uin) from uin_value_details where ds = '2023-12-26'
) as bm
select numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec_1, bm)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS uin_value_details;

select 'TEST numericIndexedVectorPointwiseLess signed twos-complement regression';

-- Before the pointwiseLess refactor the comparison used raw BSI bit planes, so
-- a negative Int32 (all-ones in two's complement) compared as larger than any
-- positive value. Rows below are picked so every key's sign pairing disagreed
-- under the old impl. See PR #88840 review thread.

DROP TABLE IF EXISTS uin_value_signed_regression;
CREATE TABLE uin_value_signed_regression
(
    bucket UInt8,
    uin UInt32,
    value Int32
) ENGINE = MergeTree ORDER BY uin;

INSERT INTO uin_value_signed_regression VALUES
    (1, 1, -1), (1, 2,  1), (1, 3, -2), (1, 4, -5), (1, 5,  3),
    (2, 1,  1), (2, 2, -1), (2, 3, -1), (2, 4, -3), (2, 5, -3);

WITH
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, bucket = 1) FROM uin_value_signed_regression
) AS vec_lhs,
(
    SELECT groupNumericIndexedVectorStateIf(uin, value, bucket = 2) FROM uin_value_signed_regression
) AS vec_rhs
SELECT arrayJoin([
    -- vec_lhs={1:-1,2:1,3:-2,4:-5,5:3}, vec_rhs={1:1,2:-1,3:-1,4:-3,5:-3}
    -- Correct signed <: keys {1,3,4}. Old unsigned-bit impl would have given {2,3,4,5}.
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_lhs, vec_rhs))
    -- All negatives strictly less than 0. Old impl returned the empty map because
    -- the negatives' bit patterns looked "large" under unsigned compare.
    , numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec_lhs, 0))
]);

DROP TABLE IF EXISTS uin_value_signed_regression;
