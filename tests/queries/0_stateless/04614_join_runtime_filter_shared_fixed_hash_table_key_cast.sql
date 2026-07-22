-- When the probe and build key types differ, the planner casts the build-side key to the common
-- type inside the join step, renaming the clause key (e.g. `_CAST(k, 'Nullable(Int32)')`). The
-- shared runtime filter descriptors must follow that rename.

DROP TABLE IF EXISTS t_build_i32;
DROP TABLE IF EXISTS t_build_u8;
DROP TABLE IF EXISTS t_probe_nullable;
DROP TABLE IF EXISTS t_probe_i32;

CREATE TABLE t_build_i32 (k Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_u8 (k UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_nullable (k Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_i32 (k Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_build_i32 SELECT number FROM numbers(100);
INSERT INTO t_build_u8 SELECT toUInt8(number) FROM numbers(50);
INSERT INTO t_probe_nullable SELECT if(number % 50 = 0, NULL, toInt32(number % 250)) FROM numbers(20000);
INSERT INTO t_probe_i32 SELECT toInt32(number % 250 - 100) FROM numbers(20000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0; -- The descriptors are not serialized with the query plan.

SET join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET enable_join_runtime_filters = 1, enable_join_fixed_hash_table_conversion = 1, join_runtime_filter_from_fixed_hash_table = 1;


-- Nullable probe + non-Nullable build: the common type Nullable(Int32) forces a cast on the build key.
SELECT '-- nullable probe, plain build';
SELECT count() FROM t_probe_nullable p INNER JOIN t_build_i32 b ON p.k = b.k
    SETTINGS log_comment = '04613_publish_nullable_probe';
SELECT count() FROM t_probe_nullable p INNER JOIN t_build_i32 b ON p.k = b.k
    SETTINGS join_runtime_filter_from_fixed_hash_table = 0;

-- Wider probe type: the common type Int32 forces a cast on the UInt8 build key.
SELECT '-- wider probe, narrow build';
SELECT count() FROM t_probe_i32 p INNER JOIN t_build_u8 b ON p.k = b.k
    SETTINGS log_comment = '04613_publish_width_cast';
SELECT count() FROM t_probe_i32 p INNER JOIN t_build_u8 b ON p.k = b.k
    SETTINGS join_runtime_filter_from_fixed_hash_table = 0;

SYSTEM FLUSH LOGS query_log, text_log;

SELECT '-- published';
SELECT uniqExact(query_id) AS published
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND message LIKE '%Published shared fixed-hash-table runtime filter%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment LIKE '04613_publish_%' AND current_database = currentDatabase()
                AND type = 'QueryFinish' AND event_date >= yesterday());

DROP TABLE t_build_i32;
DROP TABLE t_build_u8;
DROP TABLE t_probe_nullable;
DROP TABLE t_probe_i32;
