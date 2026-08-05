DROP TABLE IF EXISTS t_index_lazy_load;

CREATE TABLE t_index_lazy_load (a UInt64)
ENGINE = MergeTree  ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = '10M', primary_key_lazy_load = 1, use_primary_key_cache = 0;

INSERT INTO t_index_lazy_load SELECT number FROM numbers(15);

DETACH TABLE t_index_lazy_load;
ATTACH TABLE t_index_lazy_load;

SELECT name, primary_key_bytes_in_memory FROM system.parts WHERE database = currentDatabase() AND table = 't_index_lazy_load';

-- Check that if index is not requested it is not loaded.
SELECT part_name, mark_number, rows_in_granule FROM mergeTreeIndex(currentDatabase(), t_index_lazy_load) ORDER BY part_name, mark_number;
SELECT name, primary_key_bytes_in_memory FROM system.parts WHERE database = currentDatabase() AND table = 't_index_lazy_load';

-- If index is requested we have to load it and keep in memory.
SELECT part_name, mark_number, rows_in_granule, a FROM mergeTreeIndex(currentDatabase(), t_index_lazy_load) ORDER BY part_name, mark_number;
SELECT name, primary_key_bytes_in_memory FROM system.parts WHERE database = currentDatabase() AND table = 't_index_lazy_load';

DROP TABLE t_index_lazy_load;
