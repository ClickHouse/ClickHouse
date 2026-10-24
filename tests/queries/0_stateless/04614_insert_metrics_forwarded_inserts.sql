-- Preserve materialized-view attribution through same-query `Alias` forwarding and SQL-security
-- context replacement, but reset it when a local `Distributed` shard starts a separate forwarded `INSERT`.

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET parallel_view_processing = 0;

DROP VIEW IF EXISTS insert_metrics_mv_to_alias_mv;
DROP TABLE IF EXISTS insert_metrics_mv_to_alias_alias;
DROP TABLE IF EXISTS insert_metrics_mv_to_alias_dst;
DROP TABLE IF EXISTS insert_metrics_mv_to_alias_src;

CREATE TABLE insert_metrics_mv_to_alias_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_mv_to_alias_dst (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_mv_to_alias_alias ENGINE = Alias('insert_metrics_mv_to_alias_dst');
CREATE MATERIALIZED VIEW insert_metrics_mv_to_alias_mv TO insert_metrics_mv_to_alias_alias AS
    SELECT id FROM insert_metrics_mv_to_alias_src WHERE id < 5;

SET log_comment = '04614_mv_to_alias';
INSERT INTO insert_metrics_mv_to_alias_src SELECT number FROM numbers(10);
SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04614_mv_to_alias'
  AND query LIKE 'INSERT INTO insert_metrics_mv_to_alias_src%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

SELECT count() FROM insert_metrics_mv_to_alias_dst;

DROP VIEW insert_metrics_mv_to_alias_mv;
DROP TABLE insert_metrics_mv_to_alias_alias;
DROP TABLE insert_metrics_mv_to_alias_dst;
DROP TABLE insert_metrics_mv_to_alias_src;

SET prefer_localhost_replica = 1;
SET insert_distributed_sync = 1;

DROP VIEW IF EXISTS insert_metrics_mv_to_distributed_mv;
DROP TABLE IF EXISTS insert_metrics_mv_to_distributed;
DROP TABLE IF EXISTS insert_metrics_mv_to_distributed_dst;
DROP TABLE IF EXISTS insert_metrics_mv_to_distributed_src;

CREATE TABLE insert_metrics_mv_to_distributed_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_mv_to_distributed_dst (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_mv_to_distributed AS insert_metrics_mv_to_distributed_dst
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), insert_metrics_mv_to_distributed_dst, rand());
CREATE MATERIALIZED VIEW insert_metrics_mv_to_distributed_mv TO insert_metrics_mv_to_distributed AS
    SELECT id FROM insert_metrics_mv_to_distributed_src WHERE id < 5;

SET log_comment = '04614_mv_to_local_distributed';
INSERT INTO insert_metrics_mv_to_distributed_src SELECT number FROM numbers(10);
SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04614_mv_to_local_distributed'
  AND query LIKE 'INSERT INTO insert_metrics_mv_to_distributed_src%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

SELECT count() FROM insert_metrics_mv_to_distributed_dst;

DROP VIEW insert_metrics_mv_to_distributed_mv;
DROP TABLE insert_metrics_mv_to_distributed;
DROP TABLE insert_metrics_mv_to_distributed_dst;
DROP TABLE insert_metrics_mv_to_distributed_src;

DROP VIEW IF EXISTS insert_metrics_security_mv1;
DROP TABLE IF EXISTS insert_metrics_security_mv2_alias;
DROP VIEW IF EXISTS insert_metrics_security_mv2;
DROP TABLE IF EXISTS insert_metrics_security_dst_alias;
DROP TABLE IF EXISTS insert_metrics_security_dst;
DROP TABLE IF EXISTS insert_metrics_security_mv2_src;
DROP TABLE IF EXISTS insert_metrics_security_src;

CREATE TABLE insert_metrics_security_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_security_mv2_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_security_dst (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE insert_metrics_security_dst_alias ENGINE = Alias('insert_metrics_security_dst');
CREATE MATERIALIZED VIEW insert_metrics_security_mv2 TO insert_metrics_security_dst_alias
    DEFINER = CURRENT_USER SQL SECURITY DEFINER
    AS SELECT id FROM insert_metrics_security_mv2_src;
CREATE TABLE insert_metrics_security_mv2_alias ENGINE = Alias('insert_metrics_security_mv2');
CREATE MATERIALIZED VIEW insert_metrics_security_mv1 TO insert_metrics_security_mv2_alias AS
    SELECT id FROM insert_metrics_security_src WHERE id < 5;

SET log_comment = '04614_mv_to_definer_mv_alias';
INSERT INTO insert_metrics_security_src SELECT number FROM numbers(10);
SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'] > 0,
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04614_mv_to_definer_mv_alias'
  AND query LIKE 'INSERT INTO insert_metrics_security_src%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;

SELECT count() FROM insert_metrics_security_dst;

DROP VIEW insert_metrics_security_mv1;
DROP TABLE insert_metrics_security_mv2_alias;
DROP VIEW insert_metrics_security_mv2;
DROP TABLE insert_metrics_security_dst_alias;
DROP TABLE insert_metrics_security_dst;
DROP TABLE insert_metrics_security_mv2_src;
DROP TABLE insert_metrics_security_src;
