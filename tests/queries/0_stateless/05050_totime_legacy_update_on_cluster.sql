-- Tags: distributed, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

-- A legacy `toTime` in a standalone UPDATE / DELETE ON CLUSTER resolves as in the session.

SET distributed_ddl_output_mode = 'none';
SET use_legacy_to_time = 1;
SET enable_lightweight_update = 1;
-- The synchronous lowering, so that the count below sees the delete.
SET lightweight_delete_mode = 'lightweight_update_force';

DROP TABLE IF EXISTS t_totime_lwu;

CREATE TABLE t_totime_lwu (c0 DateTime('UTC'), v UInt32) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_totime_lwu VALUES ('2020-01-02 03:04:05', 0);

SELECT 'session', toUInt32(toTime(c0)) FROM t_totime_lwu;

UPDATE {CLICKHOUSE_DATABASE:Identifier}.t_totime_lwu ON CLUSTER test_shard_localhost
    SET v = toUInt32(toTime(c0)) WHERE 1;
SELECT 'updated_on_cluster', v FROM t_totime_lwu;

DELETE FROM {CLICKHOUSE_DATABASE:Identifier}.t_totime_lwu ON CLUSTER test_shard_localhost
    WHERE toUInt32(toTime(c0)) = 97445;
SELECT 'after_delete', count() FROM t_totime_lwu;

DROP TABLE t_totime_lwu;
