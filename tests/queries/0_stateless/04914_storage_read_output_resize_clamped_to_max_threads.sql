-- Tags: no-fasttest

-- A storage read must not build more output ports than there are threads to consume them.
-- `max_memory_usage` keeps a regression a loud query error instead of an OOM-killed server.

SET parallelize_output_from_storages = 1;
SET max_memory_usage = '2G';

DROP TABLE IF EXISTS t_resize_file;
DROP TABLE IF EXISTS t_resize_join;

-- Carrier: ReadFromFile::initializePipeline
CREATE TABLE t_resize_file (n UInt64) ENGINE = File(TabSeparated);
INSERT INTO t_resize_file VALUES (1), (2), (3);

SELECT sum(n) FROM t_resize_file
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1000000;

-- Carrier: IStorage::read (Join discards num_streams and returns a single source)
CREATE TABLE t_resize_join (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_resize_join VALUES (1, 10), (2, 20);

SELECT sum(v) FROM t_resize_join
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1000000;

-- The resize itself must survive: output is still widened to max_threads, so a fix that
-- clamps all the way down to the source count would fail here.
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT sum(n) FROM t_resize_file
    SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 1000000
) WHERE explain ILIKE '%Resize 1 %4%';

DROP TABLE t_resize_file;
DROP TABLE t_resize_join;
