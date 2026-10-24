-- `groupConcat` over a sparse column keeps the order of the rows, including the default ones.
DROP TABLE IF EXISTS t_group_concat_sparse;
CREATE TABLE t_group_concat_sparse (id UInt32, s String, fs FixedString(3))
ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO t_group_concat_sparse VALUES (1, 'a', 'a'), (2, '', ''), (3, 'b', 'b'), (4, '', ''), (5, 'c', 'c');

SELECT groupConcat('|')(s) FROM t_group_concat_sparse SETTINGS max_threads = 1, enable_parallel_replicas = 0;
SELECT hex(groupConcat('|')(fs)) = hex(groupConcat('|')(fs::String)) FROM t_group_concat_sparse SETTINGS max_threads = 1, enable_parallel_replicas = 0;
DROP TABLE t_group_concat_sparse;
