SET allow_experimental_alias_table_engine = 1;

CREATE TABLE alias_query_log_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE alias_query_log_table ENGINE = Alias(alias_query_log_source);
CREATE TABLE alias_query_log_from_alias_dst (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE alias_query_log_from_source_dst (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW alias_query_log_from_alias TO alias_query_log_from_alias_dst AS
SELECT id FROM alias_query_log_table;

CREATE MATERIALIZED VIEW alias_query_log_from_source TO alias_query_log_from_source_dst AS
SELECT id FROM alias_query_log_source;

SET log_queries_min_type = 'QUERY_FINISH';
SET log_queries = 1;

-- alias_query_log_insert
INSERT INTO alias_query_log_table SELECT 1;

SELECT
    (SELECT count() FROM alias_query_log_from_alias_dst) AS alias_view_rows,
    (SELECT count() FROM alias_query_log_from_source_dst) AS source_view_rows;

SYSTEM FLUSH LOGS query_log;

SELECT
    has(views, currentDatabase() || '.alias_query_log_from_alias') AS has_alias_view,
    has(views, currentDatabase() || '.alias_query_log_from_source') AS has_source_view
FROM system.query_log
WHERE query LIKE '-- alias_query_log_insert%INSERT INTO alias_query_log_table%'
    AND current_database = currentDatabase()
    AND event_date >= yesterday()
    AND event_time >= now() - INTERVAL 5 MINUTE
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE alias_query_log_from_alias;
DROP TABLE alias_query_log_from_source;
DROP TABLE alias_query_log_from_alias_dst;
DROP TABLE alias_query_log_from_source_dst;
DROP TABLE alias_query_log_table;
DROP TABLE alias_query_log_source;
