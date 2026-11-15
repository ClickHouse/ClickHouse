-- Tags: no-parallel

SELECT '-----------------------';
SELECT 'detached table no loop';

DROP DATABASE IF EXISTS test_no_loop;
CREATE DATABASE IF NOT EXISTS test_no_loop;

SET max_block_size = 8;
CREATE TABLE test_no_loop.t0 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop.t1 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop.t2 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop.t3 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop.t4 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop.t5 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop.t6 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop.t7 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop.t8 (c0 Int) ENGINE = MergeTree ORDER BY c0;
DETACH TABLE test_no_loop.t0;
DETACH TABLE test_no_loop.t1;
DETACH TABLE test_no_loop.t2;
DETACH TABLE test_no_loop.t3;
DETACH TABLE test_no_loop.t4;
DETACH TABLE test_no_loop.t5;
DETACH TABLE test_no_loop.t6;
DETACH TABLE test_no_loop.t7;
DETACH TABLE test_no_loop.t8;
SELECT count(*) FROM system.detached_tables WHERE database='test_no_loop';

DROP DATABASE test_no_loop;

SELECT '-----------------------';
SELECT 'max_block_size is equal to the amount of tables';

DROP DATABASE IF EXISTS test_no_loop_2;
CREATE DATABASE test_no_loop_2;

SET max_block_size = 3;
CREATE TABLE test_no_loop_2.t0 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop_2.t1 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE test_no_loop_2.t2 (c0 Int) ENGINE = MergeTree ORDER BY c0;
DETACH TABLE test_no_loop_2.t0;
DETACH TABLE test_no_loop_2.t1;
DETACH TABLE test_no_loop_2.t2;
SELECT count(*) FROM system.detached_tables WHERE database='test_no_loop_2';

DROP DATABASE test_no_loop_2;
