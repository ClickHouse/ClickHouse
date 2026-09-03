CREATE TABLE foo (key int, INDEX i1 key TYPE minmax GRANULARITY 1) Engine=MergeTree() ORDER BY key;
CREATE TABLE as_foo AS foo;
CREATE TABLE dist (key int, INDEX i1 key TYPE minmax GRANULARITY 1) Engine=Distributed(test_shard_localhost, currentDatabase(), 'foo'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE dist_as_foo Engine=Distributed(test_shard_localhost, currentDatabase(), 'foo') AS foo;

DROP TABLE foo;
DROP TABLE as_foo;
DROP TABLE dist_as_foo;
