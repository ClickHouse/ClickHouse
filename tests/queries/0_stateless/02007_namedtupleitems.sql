SELECT namedTupleItems(tuple(1, 2, 3));

DROP TABLE IF EXISTS test02007;
CREATE TABLE test02007 (
       col Tuple(
           a Tuple(key1 int, key2 int),
           b Tuple(key1 int, key3 int)
       )
) ENGINE=Memory();
INSERT INTO test02007 VALUES (tuple(tuple(1, 2), tuple(3, 4)));
INSERT INTO test02007 VALUES (tuple(tuple(5, 6), tuple(7, 8)));
 
SELECT namedTupleItems(col) FROM test02007 ORDER BY col;
DROP TABLE IF EXISTS test02007;
SELECT namedTupleItems(tuple(1, 1.3)); -- this should work, will fix

SELECT namedTupleItems(tuple(1, [1,2])); -- { serverError 43; }
SELECT namedTupleItems(tuple(1, 'a')) -- { serverError 43; }
SELECT namedTupleItems(33); -- { serverError 43; }


