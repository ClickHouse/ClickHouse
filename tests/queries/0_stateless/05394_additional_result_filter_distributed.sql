-- Tags: distributed

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_d1;
DROP TABLE IF EXISTS t_d2;
DROP TABLE IF EXISTS t_m1;
DROP TABLE IF EXISTS t_mm1;
DROP VIEW IF EXISTS t_v0;
DROP VIEW IF EXISTS t_v1;
DROP VIEW IF EXISTS t_v2;
DROP VIEW IF EXISTS t_pv;

CREATE TABLE t_local (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_local SELECT number FROM numbers(10);
CREATE TABLE t_d1 AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);
CREATE TABLE t_d2 AS t_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_local);

-- One shard: the shard processes the whole query.
SELECT number FROM remote('127.0.0.1', numbers(10)) ORDER BY number SETTINGS additional_result_filter = 'number < 3', prefer_localhost_replica = 0;
SELECT number FROM remote('127.0.0.1', numbers(10)) ORDER BY number SETTINGS additional_result_filter = 'number < 3', prefer_localhost_replica = 1;
SELECT count() AS c FROM remote('127.0.0.1', numbers(10)) SETTINGS additional_result_filter = 'c > 100';
SELECT number FROM t_d1 ORDER BY number SETTINGS additional_result_filter = 'number < 3';

-- Two shards without ORDER BY: each shard processes the whole query.
SELECT number FROM remote('127.0.0.{1,2}', numbers(10)) SETTINGS additional_result_filter = 'number = 2';
SELECT number FROM t_d2 SETTINGS additional_result_filter = 'number = 2', distributed_group_by_no_merge = 1;

-- Two shards with ORDER BY / LIMIT: the initiator finishes the query.
SELECT number FROM remote('127.0.0.{1,2}', numbers(10)) ORDER BY number SETTINGS additional_result_filter = 'number < 3';
SELECT number * 10 AS x FROM remote('127.0.0.{1,2}', numbers(10)) ORDER BY x SETTINGS additional_result_filter = 'x < 30';
SELECT number FROM remote('127.0.0.{1,2}', numbers(10)) ORDER BY number LIMIT 4 SETTINGS additional_result_filter = 'number = 1';
SELECT number FROM t_d2 ORDER BY number SETTINGS additional_result_filter = 'number < 3';

-- Two shards with aggregation: worked before, must keep working.
SELECT count() AS c FROM remote('127.0.0.{1,2}', numbers(10)) SETTINGS additional_result_filter = 'c > 100';

-- A `Merge` child plan united into the outer plan must not apply the filter a second time.
CREATE TABLE t_m1 AS t_local ENGINE = Merge(currentDatabase(), '^t_d1$');
CREATE TABLE t_mm1 AS t_local ENGINE = Merge(currentDatabase(), '^t_m1$');
SELECT number FROM t_mm1 ORDER BY number SETTINGS additional_result_filter = 'rowNumberInAllBlocks() % 2 = 0', max_threads = 1;

-- A view or `loop()` plans its own query: the caller's filter must not be applied inside it as well.
CREATE VIEW t_v0 AS SELECT number FROM t_local ORDER BY number;
CREATE VIEW t_v1 AS SELECT number FROM t_d1 ORDER BY number;
SELECT number FROM t_v0 ORDER BY number SETTINGS additional_result_filter = 'rowNumberInAllBlocks() % 2 = 0', max_threads = 1;
SELECT number FROM t_v1 ORDER BY number SETTINGS additional_result_filter = 'rowNumberInAllBlocks() % 2 = 0', max_threads = 1;
SELECT number AS x FROM t_v1 ORDER BY x SETTINGS additional_result_filter = 'x < 3';
SELECT number FROM loop(remote('127.0.0.1', numbers(10))) LIMIT 10 SETTINGS additional_result_filter = 'rowNumberInAllBlocks() % 2 = 0', max_threads = 1;

-- A subquery, or a trivial view pushed down to the shards, must not evaluate the filter against its own columns.
CREATE VIEW t_v2 AS SELECT number FROM t_d2;
SELECT number AS x FROM t_v2 ORDER BY x SETTINGS additional_result_filter = 'x < 2', optimize_trivial_view_pushdown_to_distributed = 1;
SELECT y AS z FROM (SELECT number AS y FROM remote('127.0.0.1', numbers(10))) ORDER BY z SETTINGS additional_result_filter = 'z < 3';
SELECT number AS x FROM numbers(10) WHERE number IN (SELECT number FROM remote('127.0.0.1', numbers(5))) ORDER BY x SETTINGS additional_result_filter = 'x < 3';

-- A header-only plan (the schema of `view()` or of a parameterized view) must not evaluate the filter either.
CREATE VIEW t_pv AS SELECT number FROM t_d1 WHERE number < {n:UInt64};
SELECT number AS x FROM view(SELECT number FROM remote('127.0.0.1', numbers(10))) ORDER BY x SETTINGS additional_result_filter = 'x < 3';
SELECT number AS x FROM t_pv(n = 10) ORDER BY x SETTINGS additional_result_filter = 'x < 3';

DROP VIEW t_pv;
DROP VIEW t_v2;
DROP VIEW t_v1;
DROP VIEW t_v0;
DROP TABLE t_mm1;
DROP TABLE t_m1;
DROP TABLE t_d2;
DROP TABLE t_d1;
DROP TABLE t_local;
