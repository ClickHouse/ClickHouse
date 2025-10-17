SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

DROP TABLE IF EXISTS t_sparse_distinct;

CREATE TABLE t_sparse_distinct (id UInt32, v String)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

SYSTEM STOP MERGES t_sparse_distinct;

INSERT INTO t_sparse_distinct SELECT number % 10, toString(number % 100 = 0) FROM numbers(100);
INSERT INTO t_sparse_distinct(id) SELECT number % 10 FROM numbers(100);

-- { echoOn }
SELECT name, column, serialization_kind
FROM system.parts_columns
WHERE table = 't_sparse_distinct' AND database = currentDatabase() AND column = 'v'
ORDER BY name;

set optimize_distinct_in_order=1;
set max_threads=1;

select splitByString(' ', trimLeft(explain))[1] from (explain pipeline SELECT DISTINCT id, v FROM t_sparse_distinct) where explain ilike '%DistinctSortedStreamTransform%';
SELECT DISTINCT id, v FROM t_sparse_distinct format Null;

DROP TABLE t_sparse_distinct;
