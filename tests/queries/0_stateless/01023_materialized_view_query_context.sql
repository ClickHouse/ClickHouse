-- Tags: no-parallel, no-replicated-database

-- FIXME: old analyzer does not check db exist, new one checks it and test fails. test is suppressed for replicated.
--    without analyzer:
--    2024.02.22 18:55:00.320120 [ 116105 ] {61f04f21-6d66-4064-926f-20657de2e66c} <Debug> executeQuery: (from 0.0.0.0:0, user: ) (comment: 01023_materialized_view_query_context.sql) /* ddl_entry=query-0000000009 */ CREATE MATERIALIZED VIEW test_143n70zj.mv UUID '0572ef25-139a-4705-a213-601675435648' TO test_143n70zj.output (`key` UInt64, `val` UInt64) AS SELECT key, dictGetUInt64('dict_in_01023.dict', 'val', key) AS val FROM test_143n70zj.dist_out (stage: Complete)
--    2024.02.22 18:55:00.321303 [ 116105 ] {61f04f21-6d66-4064-926f-20657de2e66c} <Debug> DDLWorker(test_143n70zj): Executed query: /* ddl_entry=query-0000000009 */ CREATE MATERIALIZED VIEW test_143n70zj.mv UUID '0572ef25-139a-4705-a213-601675435648' TO test_143n70zj.output (`key` UInt64, `val` UInt64) AS SELECT key, dictGetUInt64('dict_in_01023.dict', 'val', key) AS val FROM test_143n70zj.dist_out
--
--    with analyzer:
--    2024.02.22 19:33:36.266538 [ 108818 ] {0e1586f5-8ae0-4065-81b7-1e7d43b85d82} <Debug> executeQuery: (from 0.0.0.0:0, user: ) (comment: 01023_materialized_view_query_context.sql) /* ddl_entry=query-0000000009 */ CREATE MATERIALIZED VIEW test_devov0ke.mv UUID 'bf3a2bfe-1446-4a02-b760-bae514488c5a' TO test_devov0ke.output (`key` UInt64, `val` UInt64) AS SELECT key, dictGetUInt64('dict_in_01023.dict', 'val', key) AS val FROM test_devov0ke.dist_out (stage: Complete)
--    2024.02.22 19:33:36.266796 [ 108818 ] {0e1586f5-8ae0-4065-81b7-1e7d43b85d82} <Trace> Planner: Query SELECT __table1.key AS key, dictGetUInt64('dict_in_01023.dict', 'val', __table1.key) AS val FROM test_devov0ke.dist_out AS __table1 to stage Complete only analyze
--    2024.02.22 19:33:36.266855 [ 108818 ] {0e1586f5-8ae0-4065-81b7-1e7d43b85d82} <Trace> Planner: Query SELECT __table1.key AS key, dictGetUInt64('dict_in_01023.dict', 'val', __table1.key) AS val FROM test_devov0ke.dist_out AS __table1 from stage FetchColumns to stage Complete only analyze
--    2024.02.22 19:33:36.280740 [ 108818 ] {0e1586f5-8ae0-4065-81b7-1e7d43b85d82} <Error> executeQuery: Code: 36. DB::Exception: Dictionary (`dict_in_01023.dict`) not found. (BAD_ARGUMENTS) (version 24.2.1.1429 (official build)) (from 0.0.0.0:0) (comment: 01023_materialized_view_query_context.sql) (in query: /* ddl_entry=query-0000000009 */ CREATE MATERIALIZED VIEW test_devov0ke.mv UUID 'bf3a2bfe-1446-4a02-b760-bae514488c5a' TO test_devov0ke.output (`key` UInt64, `val` UInt64) AS SELECT key, dictGetUInt64('dict_in_01023.dict', 'val', key) AS val FROM test_devov0ke.dist_out), Stack trace (when copying this message, always include the lines below):
--    2024.02.22 19:33:36.280936 [ 108818 ] {0e1586f5-8ae0-4065-81b7-1e7d43b85d82} <Error> DDLWorker(test_devov0ke): Query /* ddl_entry=query-0000000009 */ CREATE MATERIALIZED VIEW test_devov0ke.mv UUID 'bf3a2bfe-1446-4a02-b760-bae514488c5a' TO test_devov0ke.output (`key` UInt64, `val` UInt64) AS SELECT key, dictGetUInt64('dict_in_01023.dict', 'val', key) AS val FROM test_devov0ke.dist_out wasn't finished successfully: Code: 36. DB::Exception: Dictionary (`dict_in_01023.dict`) not found. (BAD_ARGUMENTS), Stack trace (when copying this message, always include the lines below):

-- Create dictionary, since dictGet*() uses DB::Context in executeImpl()
-- (To cover scope of the Context in PushingToViews chain)

set distributed_foreground_insert=1;

DROP TABLE IF EXISTS mv;
DROP DATABASE IF EXISTS dict_in_01023;
CREATE DATABASE dict_in_01023;

CREATE TABLE dict_in_01023.input (key UInt64, val UInt64) Engine=Memory();

CREATE DICTIONARY dict_in_01023.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 1
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'input' PASSWORD '' DB 'dict_in_01023'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

CREATE TABLE input    (key UInt64) Engine=Distributed(test_shard_localhost, currentDatabase(), buffer_, key);
CREATE TABLE null_    (key UInt64) Engine=Null();
CREATE TABLE buffer_  (key UInt64) Engine=Buffer(currentDatabase(), dist_out, 1, 0, 0, 0, 0, 0, 0);
CREATE TABLE dist_out (key UInt64) Engine=Distributed(test_shard_localhost, currentDatabase(), null_, key);

CREATE TABLE output (key UInt64, val UInt64) Engine=Memory();
CREATE MATERIALIZED VIEW mv TO output AS SELECT key, dictGetUInt64('dict_in_01023.dict', 'val', key) val FROM dist_out;

INSERT INTO input VALUES (1);

SELECT count() FROM output;

DROP TABLE mv;
DROP TABLE output;
DROP TABLE dist_out;
DROP TABLE buffer_;
DROP TABLE null_;
DROP TABLE input;
DROP DICTIONARY dict_in_01023.dict;
DROP TABLE dict_in_01023.input;
DROP DATABASE dict_in_01023;
