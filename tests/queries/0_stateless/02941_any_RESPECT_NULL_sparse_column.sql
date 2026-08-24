-- regression for the case when aggregate function will be called with from==to for sparse column
DROP TABLE IF EXISTS data_sparse_column;
CREATE TABLE data_sparse_column (`key` Int64, `value` Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO data_sparse_column VALUES (1, 0);
SELECT any(value) RESPECT NULLS FROM data_sparse_column;
