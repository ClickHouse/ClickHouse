DROP TABLE IF EXISTS some_test_view;

SET join_use_nulls = 0;

CREATE OR REPLACE VIEW some_test_view
AS
SELECT * FROM ( SELECT arrayJoin([1, 2]) AS a, arrayJoin([11, 12]) AS b ) AS t1
FULL JOIN ( SELECT arrayJoin([2, 3]) AS a, arrayJoin([22, 23]) AS c ) AS t2
USING a
ORDER BY a;

SET join_use_nulls = 1;

SELECT * from some_test_view;

DROP TABLE some_test_view;
