DROP TABLE IF EXISTS nulls_first_sort_test;
CREATE TABLE nulls_first_sort_test (a Nullable(Int32), b Nullable(Int32), c Nullable(Int32)) ENGINE = Memory;

INSERT INTO nulls_first_sort_test VALUES (5,null,2), (5,null,1), (5,null,7), (5,null,3), (5,7,4), (5,7,6), (5,7,2), (5,7,1), (5,7,3), (5,7,9), (5,1,4), (5,1,6), (5,1,2), (5,1,1), (5,1,3), (5,1,9);

SELECT * FROM nulls_first_sort_test ORDER BY a NULLS FIRST,b NULLS FIRST,c NULLS FIRST LIMIT 5;
DROP TABLE nulls_first_sort_test;
