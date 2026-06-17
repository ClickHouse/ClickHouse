-- Tags: no-random-settings

SET enable_analyzer=1, join_algorithm = 'full_sorting_merge';
CREATE TABLE xxxx_yyy (key UInt32, key_b ALIAS key) ENGINE=MergeTree() ORDER BY key SETTINGS ratio_of_defaults_for_sparse_serialization=0.0;
INSERT INTO xxxx_yyy SELECT number FROM numbers(10);

SELECT *
FROM xxxx_yyy AS a
INNER JOIN xxxx_yyy AS b ON a.key = b.key_b
ORDER BY a.key;
