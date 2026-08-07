SELECT tupleConcat(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT tupleConcat((1, 'y'), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT tupleConcat((1, 'y'), (2, 'n'));
SELECT tupleConcat((1, 'y'), (2, 'n'), (3, 'n'));

WITH (1,2,3) || ('a','b','c') || ('2020-10-08'::Date, '2020-11-08'::Date) AS t
SELECT t, t.1, t.2, t.3, t.4, t.5, t.6, t.7, t.8;

DROP TABLE IF EXISTS t_02833;
CREATE TABLE t_02833 (tup Tuple(a UInt64, b UInt64)) ENGINE=Log;
INSERT INTO t_02833 VALUES ((1, 2));

WITH (tup || tup) AS res
SELECT res, res.1, res.2, res.3, res.4 FROM t_02833;

WITH (tup || (3, 4)) AS res
SELECT res, res.1, res.2, res.3, res.4 FROM t_02833;

WITH ((3, 4) || tup) AS res
SELECT res, res.1, res.2, res.3, res.4 FROM t_02833;

DROP TABLE t_02833;
