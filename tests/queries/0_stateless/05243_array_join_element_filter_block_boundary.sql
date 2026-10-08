-- ARRAY JOIN over an array longer than max_block_size, with a WHERE on the joined element
-- and the array coming from a previous ARRAY JOIN.
DROP TABLE IF EXISTS t_aj_window;
SET serialize_query_plan = 0;
SET enable_lazy_columns_replication = 1;
SET max_block_size = 6, max_threads = 1;

DROP TABLE IF EXISTS t_aj_window;
CREATE TABLE t_aj_window (id UInt32, a Array(String), b Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_aj_window SELECT number, ['p', 'q'], if(number = 1, range(16), [number]) FROM numbers(4);

SELECT count() FROM (EXPLAIN actions = 1 SELECT y FROM t_aj_window ARRAY JOIN a AS x ARRAY JOIN b AS y WHERE y % 2 = 0 SETTINGS query_plan_fuse_filter_into_array_join = 1) WHERE explain ILIKE '%Element filter column%';

-- The long row is expanded on its own, so its block is larger than max_block_size.
SELECT max(bs) FROM (SELECT blockSize() AS bs FROM t_aj_window ARRAY JOIN a AS x ARRAY JOIN b AS y WHERE y % 2 = 0 SETTINGS query_plan_fuse_filter_into_array_join = 1);
SELECT max(bs) FROM (SELECT blockSize() AS bs FROM t_aj_window ARRAY JOIN a AS x ARRAY JOIN b AS y);

SELECT count(), sum(cityHash64(id, x, y)) FROM t_aj_window ARRAY JOIN a AS x ARRAY JOIN b AS y WHERE y % 2 = 0 SETTINGS query_plan_fuse_filter_into_array_join = 1;
SELECT count(), sum(cityHash64(id, x, y)) FROM t_aj_window ARRAY JOIN a AS x ARRAY JOIN b AS y WHERE y % 2 = 0 SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count(), sum(cityHash64(id, x, y)) FROM t_aj_window ARRAY JOIN a AS x ARRAY JOIN b AS y WHERE y % 2 = 0 SETTINGS enable_lazy_columns_replication = 0;

DROP TABLE t_aj_window;
