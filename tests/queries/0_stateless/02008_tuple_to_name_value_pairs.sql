SELECT tupleToNameValuePairs(tuple(1, 2, 3));

DROP TABLE IF EXISTS test02008;
CREATE TABLE test02008 (
       col Tuple(
           a Tuple(key1 int, key2 int),
           b Tuple(key1 int, key2 int)
       )
) ENGINE=Memory();
INSERT INTO test02008 VALUES (tuple(tuple(1, 2), tuple(3, 4)));
INSERT INTO test02008 VALUES (tuple(tuple(5, 6), tuple(7, 8)));
SELECT tupleToNameValuePairs(col) FROM test02008 ORDER BY col;

DROP TABLE IF EXISTS test02008;
CREATE TABLE test02008 (
       col Tuple(CPU double, Memory double, Disk double)
) ENGINE=Memory();
INSERT INTO test02008 VALUES (tuple(3.3, 5.5, 6.6));
SELECT untuple(arrayJoin(tupleToNameValuePairs(col))) from test02008;

DROP TABLE IF EXISTS test02008;
SELECT tupleToNameValuePairs(tuple(1, 1.3)); -- { serverError 43; }
SELECT tupleToNameValuePairs(tuple(1, [1,2])); -- { serverError 43; }
SELECT tupleToNameValuePairs(tuple(1, 'a')); -- { serverError 43; }
SELECT tupleToNameValuePairs(33); -- { serverError 43; }
