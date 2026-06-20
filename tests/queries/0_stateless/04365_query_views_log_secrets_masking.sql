-- Tags: no-fasttest

DROP TABLE IF EXISTS src_04365;
DROP TABLE IF EXISTS dst_04365;
DROP TABLE IF EXISTS mv_04365;
DROP TABLE IF EXISTS src_04365_gcm;
DROP TABLE IF EXISTS dst_04365_gcm;
DROP TABLE IF EXISTS mv_04365_gcm;

CREATE TABLE src_04365 (id UInt64, data String) ENGINE = Null;
CREATE TABLE dst_04365 (id UInt64, enc String) ENGINE = Null;

CREATE MATERIALIZED VIEW mv_04365 TO dst_04365 AS
    SELECT id, encrypt('aes-128-ecb', data, '0123456789abcdef', '0123456789abcdef') AS enc
    FROM src_04365;

CREATE TABLE src_04365_gcm (id UInt64, data String) ENGINE = Null;
CREATE TABLE dst_04365_gcm (id UInt64, enc String) ENGINE = Null;

CREATE MATERIALIZED VIEW mv_04365_gcm TO dst_04365_gcm AS
    SELECT id, encrypt('aes-256-gcm', data, '0123456789abcdef0123456789abcdef', '0123456789ab') AS enc
    FROM src_04365_gcm;

INSERT INTO src_04365 VALUES (1, 'hello') SETTINGS log_queries=1, log_query_views=1;

INSERT INTO src_04365_gcm VALUES (2, 'world') SETTINGS log_queries=1, log_query_views=1;

SYSTEM FLUSH LOGS query_log, query_views_log;

-- { echo }
SELECT view_query
FROM system.query_views_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 5 MINUTE
  AND current_database = currentDatabase()
  AND view_name = currentDatabase() || '.mv_04365'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- { echo }
SELECT view_query
FROM system.query_views_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 5 MINUTE
  AND current_database = currentDatabase()
  AND view_name = currentDatabase() || '.mv_04365_gcm'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE IF EXISTS src_04365;
DROP TABLE IF EXISTS dst_04365;
DROP TABLE IF EXISTS mv_04365;
DROP TABLE IF EXISTS src_04365_gcm;
DROP TABLE IF EXISTS dst_04365_gcm;
DROP TABLE IF EXISTS mv_04365_gcm;
