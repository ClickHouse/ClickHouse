-- Tags: no-fasttest

SET allow_suspicious_low_cardinality_types = 1;

SELECT '-- negative tests';
SELECT encodeSqid(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT decodeSqid(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT encodeSqid('1'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT decodeSqid(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- const UInt*';
SELECT encodeSqid(1) AS sqid, decodeSqid(sqid);
SELECT encodeSqid(1, 2) AS sqid, decodeSqid(sqid);
SELECT encodeSqid(1, 2, 3) AS sqid, decodeSqid(sqid);
SELECT encodeSqid(1::UInt8, 2::UInt16, 3::UInt32, 4::UInt64) AS sqid, decodeSqid(sqid);
SELECT encodeSqid(toNullable(1), toLowCardinality(2)) AS sqid;

SELECT '-- non-const UInt*';
SELECT encodeSqid(materialize(1)) AS sqid, decodeSqid(sqid);
SELECT encodeSqid(materialize(1), materialize(2)) AS sqid, decodeSqid(sqid);
SELECT encodeSqid(materialize(1), materialize(2), materialize(3)) AS sqid, decodeSqid(sqid);
SELECT encodeSqid(materialize(1::UInt8), materialize(2::UInt16), materialize(3::UInt32), materialize(4::UInt64)) AS sqid, decodeSqid(sqid);
SELECT encodeSqid(toNullable(materialize(1)), toLowCardinality(materialize(2)));

SELECT '-- alias';
SELECT sqid(1, 2);
