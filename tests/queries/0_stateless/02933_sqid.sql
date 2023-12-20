-- Tags: no-fasttest

SET allow_suspicious_low_cardinality_types = 1;

SELECT '-- negative tests';
SELECT sqid(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT sqid('1'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- const UInt*';
SELECT sqid(1);
SELECT sqid(1, 2);
SELECT sqid(1, 2, 3);
SELECT sqid(1::UInt8, 2::UInt16, 3::UInt32, 4::UInt64);
SELECT sqid(toNullable(1), toLowCardinality(2));

SELECT '-- non-const UInt*';
SELECT sqid(materialize(1));
SELECT sqid(materialize(1), materialize(2));
SELECT sqid(materialize(1), materialize(2), materialize(3));
SELECT sqid(materialize(1::UInt8), materialize(2::UInt16), materialize(3::UInt32), materialize(4::UInt64));
SELECT sqid(toNullable(materialize(1)), toLowCardinality(materialize(2)));
