DROP TABLE IF EXISTS test.test;

CREATE TABLE test.test(x Int32) ENGINE = Log;
INSERT INTO test.test VALUES (123);

SELECT a1 
FROM
(
    SELECT x AS a1, x AS a2 FROM test.test
    UNION ALL
    SELECT x, x FROM test.test
);

DROP TABLE test.test;
