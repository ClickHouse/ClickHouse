-- Tags: no-tsan, no-asan, no-msan, no-debug, no-random-settings, no-replicated-database
-- memory usage can differ with sanitizers and in debug mode
-- no-replicated-database because test may fail due to adding additional shard

SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_lwu_memory SYNC;

CREATE TABLE t_lwu_memory (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_memory/', '1') ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_memory SELECT number, '' FROM numbers(5000000);
OPTIMIZE TABLE t_lwu_memory FINAL;

UPDATE t_lwu_memory SET value = toString(id) WHERE 1;
OPTIMIZE TABLE t_lwu_memory PARTITION ID 'patch-193eaced72cfb2f63d65ea2798b72338-all' FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lwu_memory' AND active = 1;
SELECT sum(id), sum(toUInt64(value)) FROM t_lwu_memory SETTINGS max_memory_usage = '150M', max_threads = 4;

DROP TABLE t_lwu_memory SYNC;
