-- The system tables that enumerate tables narrow that enumeration with the query's condition on
-- the table name, and `DatabaseTablesEnumerated` counts the tables the databases handed out, so
-- the pruning is observable here and not only in the rows, which are the same with and without it.

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_a (k UInt64, INDEX idx_k k TYPE minmax GRANULARITY 1, CONSTRAINT c_k CHECK k < 1000, PROJECTION p_k (SELECT k ORDER BY k)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_b (k UInt64, INDEX idx_k k TYPE minmax GRANULARITY 1, CONSTRAINT c_k CHECK k < 1000, PROJECTION p_k (SELECT k ORDER BY k)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_c (k UInt64, INDEX idx_k k TYPE minmax GRANULARITY 1, CONSTRAINT c_k CHECK k < 1000, PROJECTION p_k (SELECT k ORDER BY k)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_a VALUES (1);
INSERT INTO t_b VALUES (1);
INSERT INTO t_c VALUES (1);

-- The same table name in another database, to see that a condition naming the database together
-- with the table does not enumerate that name everywhere it exists.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_a (k UInt64) ENGINE = MergeTree ORDER BY k;

-- The names come from a subquery, whose set is only built when the pipeline runs. The condition
-- on `database` alone leaves one database, so only its tables can be counted.
SELECT table FROM system.parts WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 parts in subquery';
SELECT table FROM system.parts_columns WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 parts_columns in subquery';
SELECT table FROM system.projection_parts WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 projection_parts in subquery';
SELECT DISTINCT table FROM system.columns WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 columns in subquery';
SELECT table FROM system.constraints WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 constraints in subquery';
SELECT table FROM system.data_skipping_indices WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 data_skipping_indices in subquery';
SELECT table FROM system.projections WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 projections in subquery';
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN (SELECT 't_a') ORDER BY name SETTINGS log_comment = '05228 tables in subquery';

-- The control: with the in-place build of the set turned off, the enumeration is not narrowed and
-- every table of the database is handed out. The rows are the same.
SELECT table FROM system.parts WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 parts in subquery control', use_index_for_in_with_subqueries = 0;
SELECT DISTINCT table FROM system.columns WHERE database = currentDatabase() AND table IN (SELECT 't_a') ORDER BY table SETTINGS log_comment = '05228 columns in subquery control', use_index_for_in_with_subqueries = 0;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN (SELECT 't_a') ORDER BY name SETTINGS log_comment = '05228 tables in subquery control', use_index_for_in_with_subqueries = 0;

-- A condition that names the database together with the table shortlists the databases, so the
-- namesake in the other database is not enumerated.
SELECT database = currentDatabase(), table FROM system.parts WHERE (database, table) IN ((currentDatabase(), 't_a')) ORDER BY table SETTINGS log_comment = '05228 parts tuple';
SELECT database = currentDatabase(), name FROM system.tables WHERE (database, name) IN ((currentDatabase(), 't_a')) ORDER BY name SETTINGS log_comment = '05228 tables tuple';
-- And without the shortlist, both are.
SELECT database = currentDatabase(), name FROM system.tables WHERE database IN (currentDatabase(), {CLICKHOUSE_DATABASE_1:String}) AND name = 't_a' ORDER BY 1 DESC SETTINGS log_comment = '05228 tables tuple control';

SYSTEM FLUSH LOGS query_log;

SELECT '-- tables enumerated';
SELECT
    replaceOne(log_comment, '05228 ', ''),
    ProfileEvents['DatabaseTablesEnumerated']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05228 %' AND event_date >= yesterday()
ORDER BY event_time_microseconds;

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
