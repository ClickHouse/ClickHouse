-- Tags: stateful, no-parallel-replicas

set max_threads=10;
set optimize_use_implicit_projections=1;
SET optimize_use_projections = 1;
SET use_skip_indexes_on_data_read = 1;
SET optimize_trivial_count_query = 1;
EXPLAIN PIPELINE SELECT count(JavaEnable) FROM test.hits WHERE WatchID = 1 OR Title = 'next' OR URL = 'prev' OR URL = '???' OR 1 SETTINGS enable_analyzer = 0;
EXPLAIN PIPELINE SELECT count(JavaEnable) FROM test.hits WHERE WatchID = 1 OR Title = 'next' OR URL = 'prev' OR URL = '???' OR 1 SETTINGS enable_analyzer = 1;
