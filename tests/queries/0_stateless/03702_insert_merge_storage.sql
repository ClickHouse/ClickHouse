CREATE TABLE test_mt_1 (a Int32, b String) Engine=MergeTree ORDER BY a;
CREATE TABLE test_mt_2 (a Int64, b String) Engine=MergeTree ORDER BY a;

INSERT INTO test_mt_1 VALUES(1, 'test_1');
INSERT INTO test_mt_2 VALUES(2, 'test_2');

CREATE TABLE test_merge Engine=Merge(database(), 'test_mt_*');
SELECT * FROM test_merge ORDER BY ALL;
INSERT INTO test_merge VALUES (3, 'test_3'); -- { serverError TABLE_IS_READ_ONLY }

SELECT '--- INSERT explicit';
REPLACE TABLE test_merge Engine=Merge(database(), 'test_mt_*', test_mt_1);
INSERT INTO test_merge VALUES (3, 'test_3');
SELECT * FROM test_mt_1 ORDER BY ALL;

SELECT '--- INSERT explicit fully qualified';
REPLACE TABLE test_merge Engine=Merge(database(), 'test_mt_*', database() || '.test_mt_2');
INSERT INTO test_merge VALUES (4, 'test_4');
SELECT * FROM test_mt_2 ORDER BY ALL;
SELECT '--- explicit table to write is serialized as literal';
SHOW CREATE TABLE test_merge;

-- INSERT table doesn't exists
REPLACE TABLE test_merge Engine=Merge(database(), 'test_mt_*', test_mt_not_exists);
INSERT INTO test_merge VALUES (0, 'failed'); -- { serverError UNKNOWN_TABLE }

-- INSERT table doesn't match
REPLACE TABLE test_merge Engine=Merge(database(), 'test_mt_*', invalid_db.test_mt_1); -- { serverError BAD_ARGUMENTS }
REPLACE TABLE test_merge Engine=Merge(database(), 'test_mt_*', test_invalid); -- { serverError BAD_ARGUMENTS }

-- database regexp with implicit insert database is forbidden
REPLACE TABLE test_merge Engine=Merge(REGEXP({CLICKHOUSE_DATABASE:String}), 'test_mt_*', test_mt_1); -- { serverError BAD_ARGUMENTS }

-- database regexp with fully qualified insert table
REPLACE TABLE test_merge Engine=Merge(REGEXP({CLICKHOUSE_DATABASE:String}), 'test_mt_*', database() || '.test_mt_2');

SELECT '--- INSERT auto';
REPLACE TABLE test_merge Engine=Merge(database(), 'test_mt_*', auto);
INSERT INTO test_merge VALUES (5, 'test_5');
SELECT * FROM test_mt_2 ORDER BY ALL;
SELECT '--- auto is serialized as identifier';
SHOW CREATE TABLE test_merge;
SELECT '--- INSERT auto in the latest name';
CREATE TABLE test_mt_3 (a Int128, b String) Engine=MergeTree ORDER BY a;
INSERT INTO test_merge VALUES (6, 'test_6');
SELECT * FROM test_mt_3 ORDER BY ALL;

SELECT '--- INSERT triggers Materialized view on inner table';
CREATE MATERIALIZED VIEW test_mat_view_inner Engine=Memory AS SELECT a*a AS id, b AS val FROM test_mt_3;
INSERT INTO test_merge VALUES (7, 'test_7');
SELECT * FROM test_mat_view_inner ORDER BY ALL;

SELECT '--- INSERT triggers Materialized view on merge table';
CREATE MATERIALIZED VIEW test_mat_view_merge Engine=Memory AS SELECT a*a AS id, b AS val FROM test_merge;
INSERT INTO test_merge VALUES (8, 'test_8');
SELECT * FROM test_mat_view_merge ORDER BY ALL;

SELECT '--- INSERT in chain of Merge tables';
CREATE TABLE test_chain_merge Engine=Merge(database(), 'test_merge', auto);
INSERT INTO test_chain_merge VALUES (9, 'test_9');
SELECT * FROM test_mt_3 ORDER BY ALL;

SELECT '--- INSERT in Merge with materialized view';
CREATE TABLE test_source (a Int) Engine=Memory;
CREATE MATERIALIZED VIEW test_mat_view_to_merge TO test_merge AS SELECT a*a AS a, 'test_' || toString(a) AS b FROM test_source;
INSERT INTO test_source VALUES(10);
SELECT * FROM test_mt_3 ORDER BY ALL;

-- Detect insert loop
CREATE TABLE test_cycle_1 (a Int) Engine=Merge(database(), 'test_cycle.*', test_cycle_2);
CREATE TABLE test_cycle_2 Engine=Merge(database(), 'test_cycle.*', test_cycle_1);
INSERT INTO test_cycle_1 VALUES (1); -- { serverError TOO_DEEP_RECURSION }

-- Type check
CREATE TABLE test_types_1 (name String) ORDER BY name;
CREATE TABLE test_types_2 (name UInt32) ORDER BY name;
CREATE TABLE merge_test_types ENGINE = Merge(database(), 'test_types_.*', auto);
INSERT INTO merge_test_types VALUES('Mark'); -- { serverError CANNOT_PARSE_TEXT }

SELECT '--- Merge table defaults used';
CREATE TABLE test_merge_defaults_mt (a Int32, b String) Engine=MergeTree ORDER BY a;
CREATE TABLE test_merge_defaults (a Int16 DEFAULT 1, b String DEFAULT toString(a)) Engine=Merge(database(), 'test_merge_defaults_mt', test_merge_defaults_mt);
INSERT INTO test_merge_defaults (a) VALUES (NULL);
SELECT * FROM test_merge_defaults_mt ORDER BY ALL;

SELECT '--- Inner table defaults are used';
CREATE TABLE test_inner_defaults_mt (a Int32, b String DEFAULT toString(a+1)) Engine=MergeTree ORDER BY a;
CREATE TABLE test_inner_defaults (a Int16 DEFAULT 1) Engine=Merge(database(), 'test_inner_defaults_mt', test_inner_defaults_mt);
INSERT INTO test_inner_defaults VALUES (NULL);
SELECT * FROM test_inner_defaults_mt ORDER BY ALL;

-- Inner constraints are validated
CREATE TABLE test_constraints_1 (id Int, CONSTRAINT small_id CHECK  id < 10) ORDER BY id;
CREATE TABLE test_merge_constraints (id Int) ENGINE = Merge(database(), 'test_constraints_*', auto);
INSERT INTO test_merge_constraints VALUES (11); -- { serverError VIOLATED_CONSTRAINT }

-- Merge constraints are validated
REPLACE TABLE test_merge_constraints (id Int, CONSTRAINT small_id CHECK  id > 5) ENGINE = Merge(database(), 'test_constraints_*', auto);
INSERT INTO test_merge_constraints VALUES (4); -- { serverError VIOLATED_CONSTRAINT }

SELECT '--- All constraints passed';
INSERT INTO test_merge_constraints VALUES (6);
SELECT * FROM test_merge_constraints ORDER BY ALL;
