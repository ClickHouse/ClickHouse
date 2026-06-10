-- Tags: zookeeper, no-replicated-database, no-parallel, no-ordinary-database

SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS rmt;
DROP TABLE IF EXISTS rmt1;
DROP TABLE IF EXISTS rmt2;
DROP TABLE IF EXISTS rmt3;

SET database_replicated_allow_replicated_engine_arguments=1;

CREATE TABLE rmt (n UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/test_01148/{shard}/{database}/{table}', '{replica}') ORDER BY n;
SHOW CREATE TABLE rmt;
RENAME TABLE rmt TO rmt1;
DETACH TABLE rmt1;
ATTACH TABLE rmt1;
SHOW CREATE TABLE rmt1;

CREATE TABLE rmt (n UInt64, s String) ENGINE = ReplicatedMergeTree('{default_path_test}{uuid}', '{default_name_test}') ORDER BY n;    -- { serverError BAD_ARGUMENTS }
CREATE TABLE rmt (n UInt64, s String) ENGINE = ReplicatedMergeTree('{default_path_test}test_01148', '{default_name_test}') ORDER BY n;
SHOW CREATE TABLE rmt;
RENAME TABLE rmt TO rmt2;   -- { serverError NOT_IMPLEMENTED }
DETACH TABLE rmt;
ATTACH TABLE rmt;
SHOW CREATE TABLE rmt;

SET distributed_ddl_output_mode='none';
DROP DATABASE IF EXISTS test_01148_atomic;
CREATE DATABASE test_01148_atomic ENGINE=Atomic;
CREATE TABLE test_01148_atomic.rmt2 ON CLUSTER test_shard_localhost (n int, PRIMARY KEY n) ENGINE=ReplicatedMergeTree;
CREATE TABLE test_01148_atomic.rmt3 AS test_01148_atomic.rmt2; -- { serverError BAD_ARGUMENTS }
CREATE TABLE test_01148_atomic.rmt4 ON CLUSTER test_shard_localhost AS test_01148_atomic.rmt2;
SHOW CREATE TABLE test_01148_atomic.rmt2;
RENAME TABLE test_01148_atomic.rmt4 to test_01148_atomic.rmt3;
SHOW CREATE TABLE test_01148_atomic.rmt3;

DROP DATABASE IF EXISTS test_01148_ordinary;
set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE test_01148_ordinary ENGINE=Ordinary;
RENAME TABLE test_01148_atomic.rmt3 to test_01148_ordinary.rmt3; -- { serverError NOT_IMPLEMENTED }
DROP DATABASE test_01148_ordinary;
DROP DATABASE test_01148_atomic;

DROP TABLE rmt;
DROP TABLE rmt1;

DROP DATABASE IF EXISTS imdb_01148;
CREATE DATABASE imdb_01148 ENGINE = Replicated('/test/databases/imdb_01148', '{shard}', '{replica}');
CREATE TABLE imdb_01148.movie_directors (`director_id` UInt64, `movie_id` UInt64) ENGINE = ReplicatedMergeTree ORDER BY (director_id, movie_id) SETTINGS index_granularity = 8192;
CREATE TABLE imdb_01148.anything AS imdb_01148.movie_directors;
SHOW CREATE TABLE imdb_01148.anything;
DROP DATABASE imdb_01148;
