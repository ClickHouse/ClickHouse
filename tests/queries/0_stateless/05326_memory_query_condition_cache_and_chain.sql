-- A conjunction in `WHERE` is evaluated by a chain of `FilterTransform`s (one per atom) with the same key
-- of the query condition cache. The atoms after the first one get the chunks of a `Memory` table with some
-- rows already removed, so they must not record the granules by the positions of the rows.

SET use_query_condition_cache = 1, optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS t_memory_qcc_and;
CREATE TABLE t_memory_qcc_and (a Int32, b Int32, c Int32) ENGINE = Memory;
INSERT INTO t_memory_qcc_and SELECT number % 7, number % 11, number % 13 FROM numbers(200000) SETTINGS max_block_size = 100000;

SELECT count() FROM t_memory_qcc_and WHERE a = b AND c = a;
SELECT count() FROM t_memory_qcc_and WHERE a = b AND c = a;
SELECT count() FROM t_memory_qcc_and WHERE a = b AND c = a AND b < 3;
SELECT count() FROM t_memory_qcc_and WHERE a = b AND c = a AND b < 3;
SELECT count() FROM t_memory_qcc_and WHERE a = b AND c = a AND b < 3 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_memory_qcc_and;
