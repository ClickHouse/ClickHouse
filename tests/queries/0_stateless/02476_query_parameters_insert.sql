DROP TABLE IF EXISTS 02476_query_parameters_insert;
CREATE TABLE 02476_query_parameters_insert (x Int32) ENGINE=MergeTree() ORDER BY tuple();

SET param_x = 1;
INSERT INTO 02476_query_parameters_insert VALUES ({x: Int32});
SELECT * FROM 02476_query_parameters_insert;

DROP TABLE 02476_query_parameters_insert;
