-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS test_tuple_membership_packed;

CREATE TABLE test_tuple_membership_packed (s String, x UInt8) ENGINE = MergeTree
ORDER BY tuple(s, x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tuple_membership_packed VALUES ('a', 1), ('b', 5), ('c', 9);

SELECT count() FROM test_tuple_membership_packed WHERE (s, x) IN (('b', 5)) SETTINGS force_primary_key = 1;

SELECT count() FROM test_tuple_membership_packed WHERE (s, x) NOT IN (('b', 5)) SETTINGS force_primary_key = 1;

SET optimize_rewrite_has_to_in = 0;

SELECT count() FROM test_tuple_membership_packed WHERE has([('b', 5)], (s, x)) SETTINGS force_primary_key = 1;

DROP TABLE test_tuple_membership_packed;
