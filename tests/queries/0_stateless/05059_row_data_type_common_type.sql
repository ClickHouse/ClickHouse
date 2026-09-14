SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_common_type;
CREATE TABLE row_common_type (a UInt64, r Row(x UInt64, y String), r2 Row(x UInt64, y String)) ENGINE = MergeTree ORDER BY a;
INSERT INTO row_common_type VALUES (1, (1, 'a'), (10, 'p')), (2, (2, 'b'), (20, 'q'));

-- The common type of a Row and a Tuple is found for the equivalent named Tuple.
SELECT a, if(a % 2 = 0, r, (0, 'z')) AS v, toTypeName(v) FROM row_common_type ORDER BY a;

-- Two distinct columns of the same Row type keep the Row type through if.
SELECT a, if(a % 2 = 0, r, r2) AS v, toTypeName(v) FROM row_common_type ORDER BY a;

SELECT a, multiIf(a = 1, r, a = 2, (9, 'q'), (0, 'z')) AS v, toTypeName(v) FROM row_common_type ORDER BY a;

SELECT a, least(r, (1, 'z')) AS l, greatest(r, (1, 'z')) AS g, toTypeName(l) FROM row_common_type ORDER BY a;

-- arrayIntersect goes through the most common subtype.
SELECT a, arrayIntersect([r], [(1, 'a')]) FROM row_common_type ORDER BY a;

DROP TABLE row_common_type;
