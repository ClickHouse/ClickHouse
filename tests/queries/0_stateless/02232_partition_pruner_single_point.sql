DROP TABLE IF EXISTS lower_test;

CREATE TABLE lower_test (
    a Int32,
    b String
) ENGINE=MergeTree
PARTITION BY b
ORDER BY a;

INSERT INTO lower_test (a,b) VALUES (1,'A'),(2,'B'),(3,'C');

SELECT a FROM lower_test WHERE lower(b) IN ('a','b') order by a;

DROP TABLE lower_test;
