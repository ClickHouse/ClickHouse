-- A remote block with several columns of the same name used to lose all but the first one
-- when it was marshalled in parallel (`ColumnBLOB`), which led to a segmentation fault.

DROP TABLE IF EXISTS t_remote_duplicate_names;
CREATE TABLE t_remote_duplicate_names (a UInt64, b UInt64, c UInt64, d UInt64, e UInt64) ENGINE = Memory;

SET enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0, max_threads = 3;

INSERT INTO t_remote_duplicate_names SELECT number % 3, number, number, number, number FROM remote('127.0.0.{1,2}', system.numbers) LIMIT 256;
SELECT count(), sum(b = c AND c = d AND d = e) FROM t_remote_duplicate_names;

DROP TABLE t_remote_duplicate_names;
