-- Tags: no-fasttest

SET allow_suspicious_low_cardinality_types = 1;

SELECT '-- negative tests';
SELECT sqidEncode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT sqidDecode(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT sqidEncode('1'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sqidDecode(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- const UInt*';
SELECT sqidEncode(1) AS sqid, sqidDecode(sqid);
SELECT sqidEncode(1, 2) AS sqid, sqidDecode(sqid);
SELECT sqidEncode(1, 2, 3) AS sqid, sqidDecode(sqid);
SELECT sqidEncode(1::UInt8, 2::UInt16, 3::UInt32, 4::UInt64) AS sqid, sqidDecode(sqid);
SELECT sqidEncode(toNullable(1), toLowCardinality(2)) AS sqid;
SELECT sqidDecode('1');

SELECT '-- non-const UInt*';
SELECT sqidEncode(materialize(1)) AS sqid, sqidDecode(sqid);
SELECT sqidEncode(materialize(1), materialize(2)) AS sqid, sqidDecode(sqid);
SELECT sqidEncode(materialize(1), materialize(2), materialize(3)) AS sqid, sqidDecode(sqid);
SELECT sqidEncode(materialize(1::UInt8), materialize(2::UInt16), materialize(3::UInt32), materialize(4::UInt64)) AS sqid, sqidDecode(sqid);
SELECT sqidEncode(toNullable(materialize(1)), toLowCardinality(materialize(2)));

SELECT '-- invalid sqid';
SELECT sqidDecode('invalid sqid');

SELECT '-- bug 69450';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab SELECT * FROM generateRandom() LIMIT 1000000;
SELECT sqidDecode(id) FROM tab FORMAT Null;
DROP TABLE tab;

SELECT '-- alias';
SELECT sqid(1, 2);
