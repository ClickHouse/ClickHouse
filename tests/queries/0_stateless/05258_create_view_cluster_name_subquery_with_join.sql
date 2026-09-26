-- A view reads through `cluster` with a cluster name computed by a scalar subquery with a JOIN.
-- Creating it evaluates the subquery to find the tables the view depends on: this must not crash the server
-- and must still register the dependency on the table read through `cluster`.

DROP VIEW IF EXISTS v_05258;
DROP TABLE IF EXISTS t_probe_05258;
DROP TABLE IF EXISTS t_build_05258;
DROP TABLE IF EXISTS t_dep_05258;

CREATE TABLE t_probe_05258 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_probe_05258 SELECT number FROM numbers(10000);
CREATE TABLE t_build_05258 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_build_05258 SELECT number * 1000 FROM numbers(10);
CREATE TABLE t_dep_05258 (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE VIEW v_05258 (k UInt64) AS SELECT k FROM cluster((SELECT if(count() > 0, 'test_shard_localhost', '') FROM t_probe_05258, t_build_05258 WHERE t_probe_05258.k = t_build_05258.k SETTINGS enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1), currentDatabase(), 't_dep_05258');
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'v_05258';

SET check_referential_table_dependencies = 1;
DROP TABLE t_dep_05258; -- { serverError HAVE_DEPENDENT_OBJECTS }

DROP VIEW v_05258;
DROP TABLE t_dep_05258;
DROP TABLE t_build_05258;
DROP TABLE t_probe_05258;
