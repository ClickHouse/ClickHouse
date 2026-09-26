-- A `Merge` read protects arbitrary child storages from excessive source counts. The aggregate
-- guard must nevertheless honor a trivial `LIMIT`: `GenerateRandom` reduces the same request to
-- `ceil(LIMIT / max_block_size)` sources, so rejecting it before child planning would turn a safe query into
-- `PARAMETER_OUT_OF_BOUND`.
DROP TABLE IF EXISTS t_merge_generate_random_limit;
DROP TABLE IF EXISTS t_generate_random_limit_child;

CREATE TABLE t_generate_random_limit_child (n UInt64) ENGINE = GenerateRandom(1);
CREATE TABLE t_merge_generate_random_limit (n UInt64)
ENGINE = Merge(currentDatabase(), '^t_generate_random_limit_child$');

SELECT count() FROM (SELECT n FROM t_merge_generate_random_limit LIMIT 100000)
SETTINGS optimize_trivial_count_query = 0, max_threads = 4, max_streams_to_max_threads_ratio = 1073741824, max_block_size = 65536;

DROP TABLE t_merge_generate_random_limit;
DROP TABLE t_generate_random_limit_child;
