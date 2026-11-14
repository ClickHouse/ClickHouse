-- Tags: no-replicated-database
-- Tag no-replicated-database: `CREATE CLONE AS is not supported with Replicated databases`

-- `CLONE AS` reuses the source parts, which are physically sorted in the original order,
-- so its ORDER BY must be left alone even with `force_primary_key_reverse_order` enabled.
DROP TABLE IF EXISTS t_force_reverse_clone_src;
DROP TABLE IF EXISTS t_force_reverse_clone_dst;
SET force_primary_key_reverse_order = 0;
CREATE TABLE t_force_reverse_clone_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_force_reverse_clone_src SELECT number FROM numbers(10);
SET force_primary_key_reverse_order = 1;
CREATE TABLE t_force_reverse_clone_dst CLONE AS t_force_reverse_clone_src;
SELECT create_table_query LIKE '%ORDER BY x DESC%' FROM system.tables
    WHERE database = currentDatabase() AND name = 't_force_reverse_clone_dst';
SELECT x FROM t_force_reverse_clone_dst ORDER BY x ASC;

DROP TABLE t_force_reverse_clone_src;
DROP TABLE t_force_reverse_clone_dst;
