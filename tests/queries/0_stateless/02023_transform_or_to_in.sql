DROP TABLE IF EXISTS t_transform_or;

CREATE TABLE t_transform_or(B AggregateFunction(uniq, String), A String) Engine=MergeTree ORDER BY (A);

INSERT INTO t_transform_or SELECT uniqState(''), '0';

SELECT uniqMergeIf(B, (A = '1') OR (A = '2') OR (A = '3'))
FROM cluster(test_cluster_two_shards, currentDatabase(), t_transform_or)
SETTINGS legacy_column_name_of_tuple_literal = 0;

SELECT uniqMergeIf(B, (A = '1') OR (A = '2') OR (A = '3'))
FROM cluster(test_cluster_two_shards, currentDatabase(), t_transform_or)
SETTINGS legacy_column_name_of_tuple_literal = 1;

DROP TABLE t_transform_or;
