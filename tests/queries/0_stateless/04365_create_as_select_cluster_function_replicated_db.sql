-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: needs a cluster function
-- no-replicated-database: the test creates its own Replicated database and checks its DDL

-- CREATE TABLE ... AS SELECT FROM <cluster function> used to fail inside a Replicated
-- database with "Not found column ... in block ." (NOT_FOUND_COLUMN_IN_BLOCK). The
-- Replicated DDL worker runs the query with query_kind = NO_QUERY (not INITIAL_QUERY),
-- so IStorageCluster::getQueryProcessingStage wrongly treated it as a follower and stopped
-- the local plan at FetchColumns, producing an empty header. See issue #107057.

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier}
    ENGINE = Replicated('/clickhouse/{database}/04365_repl', 'shard1', 'replica1') FORMAT Null;

-- The data file lives in the shared user_files dir, so its name must be unique per test
-- run or parallel copies clobber each other's file and fileCluster reads the union.
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_04365_data.tsv', 'TSV', 'id UInt64, value String')
SELECT number, toString(number) FROM numbers(5)
SETTINGS engine_file_truncate_on_insert = 1;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t
    ENGINE = MergeTree() ORDER BY id
    AS SELECT id, value FROM fileCluster('test_shard_localhost', {CLICKHOUSE_DATABASE:String} || '_04365_data.tsv', 'TSV', 'id UInt64, value String') FORMAT Null;

SELECT count(), sum(id) FROM {CLICKHOUSE_DATABASE:Identifier}.t;

-- Positional arguments must also be resolved on the Replicated DDL worker (query_kind =
-- NO_QUERY): QueryAnalyzer::replaceNodesWithPositionalArguments gated on == INITIAL_QUERY,
-- so GROUP BY 1 was left unresolved and threw NOT_AN_AGGREGATE.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_pos
    ENGINE = MergeTree() ORDER BY id
    AS SELECT id, count() AS c FROM fileCluster('test_shard_localhost', {CLICKHOUSE_DATABASE:String} || '_04365_data.tsv', 'TSV', 'id UInt64, value String') GROUP BY 1 FORMAT Null;

SELECT count(), sum(id) FROM {CLICKHOUSE_DATABASE:Identifier}.t_pos;
