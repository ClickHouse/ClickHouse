DROP TABLE IF EXISTS _02476_query_parameters_insert;
CREATE TABLE _02476_query_parameters_insert (x Int32) ENGINE=MergeTree() ORDER BY tuple();

SET param_x = 1;
INSERT INTO _02476_query_parameters_insert VALUES ({x: Int32});
SELECT * FROM _02476_query_parameters_insert;

DROP TABLE _02476_query_parameters_insert;
