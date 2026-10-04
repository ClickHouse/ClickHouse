-- Tags: no-replicated-database
-- no-replicated-database: TRUNCATE ALL TABLES is not supported for Replicated databases.

-- `TRUNCATE ALL TABLES FROM db` used to fail for the whole database when it held a table whose
-- engine stores no data and therefore never implemented `truncate()`. Such engines are now skipped,
-- as views and dictionaries already are: https://github.com/ClickHouse/ClickHouse/pull/100943

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE plain (x UInt64) ENGINE = MergeTree ORDER BY x;

CREATE TABLE e_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^plain$');
CREATE TABLE e_null (x UInt64) ENGINE = Null;
CREATE TABLE e_generate_random (x UInt64) ENGINE = GenerateRandom;
CREATE TABLE e_executable (x UInt64) ENGINE = Executable('05236_no_such_script.sh', TSV);
CREATE TABLE e_executable_pool (x UInt64) ENGINE = ExecutablePool('05236_no_such_script.sh', TSV);
CREATE TABLE e_fuzz_query (q String) ENGINE = FuzzQuery('SELECT 1');
CREATE TABLE e_fuzz_json (j String) ENGINE = FuzzJSON('{"a": 1}');
CREATE TABLE e_url (x UInt64) ENGINE = URL('http://127.0.0.1:1/05236', TSV);

CREATE TABLE t_log ENGINE = Log AS SELECT 1 AS x;
CREATE TABLE t_tiny_log ENGINE = TinyLog AS SELECT 1 AS x;
CREATE TABLE t_stripe_log ENGINE = StripeLog AS SELECT 1 AS x;
CREATE TABLE t_memory ENGINE = Memory AS SELECT 1 AS x;
CREATE TABLE t_set ENGINE = Set AS SELECT 1 AS x;
CREATE TABLE t_join ENGINE = Join(ANY, LEFT, x) AS SELECT 1 AS x, 1 AS y;
CREATE TABLE t_file ENGINE = File(TSV) AS SELECT 1 AS x;
CREATE MATERIALIZED VIEW t_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM plain;

-- A `Set` table rejects a read, and `count()` on it only survives while `optimize_trivial_count_query`
-- answers from metadata, so it is probed through `IN`, the access path the engine actually supports.
CREATE VIEW holding_rows AS
SELECT arraySort(groupArray(t)) AS tables
FROM
(
    SELECT 't_log' AS t, count() AS c FROM t_log
    UNION ALL SELECT 't_tiny_log', count() FROM t_tiny_log
    UNION ALL SELECT 't_stripe_log', count() FROM t_stripe_log
    UNION ALL SELECT 't_memory', count() FROM t_memory
    UNION ALL SELECT 't_join', count() FROM t_join
    UNION ALL SELECT 't_file', count() FROM t_file
    UNION ALL SELECT 't_mv', count() FROM t_mv
    UNION ALL SELECT 't_set', toUInt64(1 IN t_set)
)
WHERE c > 0;

INSERT INTO plain SELECT number FROM numbers(300);

SELECT 'plain before', count() FROM plain;
SELECT 'holding rows before', tables FROM holding_rows;

TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE:Identifier};

SELECT 'plain after', count() FROM plain;
SELECT 'holding rows after', tables FROM holding_rows;

INSERT INTO plain SELECT number FROM numbers(300);
TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE:Identifier} LIKE '%';
SELECT 'plain after LIKE', count() FROM plain;

TRUNCATE TABLE e_merge; -- { serverError NOT_IMPLEMENTED }
TRUNCATE TABLE e_null; -- { serverError NOT_IMPLEMENTED }
TRUNCATE TABLE e_url; -- { serverError NOT_IMPLEMENTED }

-- An engine that holds rows of its own is not skipped: Buffer keeps failing the statement, because
-- omitting it would report success while its buffered rows survive.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dest (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.buf (x UInt64) ENGINE = Buffer({CLICKHOUSE_DATABASE_1:Identifier}, dest, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError NOT_IMPLEMENTED }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
