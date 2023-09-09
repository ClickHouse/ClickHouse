DROP TABLE IF EXISTS src_02888_1;
DROP TABLE IF EXISTS mv_02888_1;

DROP TABLE IF EXISTS src_02888_2;
DROP TABLE IF EXISTS mv_02888_2;

CREATE TABLE src_02888_1 (id UInt8) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_02888_1 ENGINE = Memory AS SELECT id FROM src_02888_1 WHERE id % 2 = 0;

CREATE TABLE src_02888_2 (id UInt8) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_02888_2 ENGINE = Memory AS SELECT id FROM src_02888_2 WHERE id % 2 = 0;

SET parallel_view_query_execution = 1;
SET log_queries = 1;
SET log_query_threads = 1;
SET log_queries_min_type = 'QUERY_FINISH';
INSERT INTO src_02888_1 VALUES (1), (2);
SET log_queries = 0;
SET log_query_threads = 0;
SYSTEM FLUSH LOGS;

SELECT COUNT() > 1 FROM system.query_thread_log WHERE query LIKE 'INSERT INTO src_02888_1 VALUES%' AND current_database = currentDatabase();

SELECT * FROM mv_02888_1 ORDER BY id;

SET parallel_view_query_execution = 0;
SET log_queries = 1;
SET log_query_threads = 1;
INSERT INTO src_02888_2 VALUES (3), (4);
SET log_queries = 0;
SET log_query_threads = 0;
SYSTEM FLUSH LOGS;

SELECT COUNT() > 1 FROM system.query_thread_log WHERE query LIKE 'INSERT INTO src_02888_2 VALUES%' AND current_database = currentDatabase();

SELECT * FROM mv_02888_2 ORDER BY id;

DROP TABLE mv_02888_1;
DROP TABLE src_02888_1;

DROP TABLE mv_02888_2;
DROP TABLE src_02888_2;
