SET enable_analyzer = 1;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test
SET query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1;
SET query_plan_join_shard_by_pk_ranges = 0; -- adds 'Sharding:' lines to EXPLAIN output when enabled
SET query_plan_merge_filter_into_join_condition = 0; -- absorbing WHERE into ON clause prevents outer→inner join conversion

DROP TABLE IF EXISTS test_table_1;
CREATE TABLE test_table_1
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id
SETTINGS index_granularity = 16 # We have number of granules in the `EXPLAIN` output in reference file
;

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id
SETTINGS index_granularity = 16
;

INSERT INTO test_table_1 VALUES (1, 'Value_1'), (2, 'Value_2');
INSERT INTO test_table_2 VALUES (2, 'Value_2'), (3, 'Value_3');


EXPLAIN header = 1, actions = 1 SELECT * FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE rhs.id != 0
SETTINGS query_plan_join_swap_table = 'false', enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 0, query_plan_convert_outer_join_to_inner_join = 1 -- CI may inject False; outer join not converted to inner join, WHERE predicate stays above join instead of being pushed to prewhere
;

SELECT '--';

SELECT * FROM test_table_1 AS lhs LEFT JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE rhs.id != 0;

SELECT '--';

EXPLAIN header = 1, actions = 1 SELECT * FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0
SETTINGS query_plan_join_swap_table = 'false', enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 0, query_plan_convert_outer_join_to_inner_join = 1 -- CI may inject False; outer join not converted to inner join, WHERE predicate stays above join instead of being pushed to prewhere
;

SELECT '--';

SELECT * FROM test_table_1 AS lhs RIGHT JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0;

SELECT '--';

EXPLAIN header = 1, actions = 1 SELECT * FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0 AND rhs.id != 0
SETTINGS query_plan_join_swap_table = 'false', enable_join_runtime_filters = 0, query_plan_merge_filter_into_join_condition = 0, query_plan_convert_outer_join_to_inner_join = 1 -- CI may inject False; outer join not converted to inner join, WHERE predicate stays above join instead of being pushed to prewhere
;

SELECT '--';

SELECT * FROM test_table_1 AS lhs FULL JOIN test_table_2 AS rhs ON lhs.id = rhs.id WHERE lhs.id != 0 AND rhs.id != 0;

DROP TABLE test_table_1;
DROP TABLE test_table_2;
