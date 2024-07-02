DROP TABLE IF EXISTS t_sparse_column_name;

CREATE TABLE t_sparse_column_name(a String, b String) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_sparse_column_name SELECT '', 'aaa' FROM numbers(1000);

SELECT DISTINCT toColumnTypeName(a), toColumnTypeName(b), toColumnTypeName('aaa') FROM t_sparse_column_name;

DROP TABLE t_sparse_column_name;
