-- Tags: no-parallel-replicas
-- A lazy column added by `ALTER` whose `DEFAULT`/`MATERIALIZED` expression reads the point-read vector column. The part
-- predates the column, so the value has to be synthesized from `vec` at read time rather than read from disk.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_pr_default_from_vec;

CREATE TABLE quantize_pr_default_from_vec
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Vectors correlate with the sort key, so neighbour order is unambiguous.
INSERT INTO quantize_pr_default_from_vec
SELECT number, arrayMap(j -> toFloat32(number / 1000.0 + (sipHash64(number, j) % 100) / 1000.0), range(64))
FROM numbers(2000);

ALTER TABLE quantize_pr_default_from_vec ADD COLUMN norm Float32 DEFAULT vec[1];
ALTER TABLE quantize_pr_default_from_vec ADD COLUMN norm_mat Float32 MATERIALIZED vec[2];

-- The point of the test: the part stores neither added column, so both must be evaluated from `vec`.
SELECT 'added_columns_absent_from_part', count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'quantize_pr_default_from_vec' AND active
  AND column IN ('norm', 'norm_mat') AND column_bytes_on_disk > 0;

-- A `DEFAULT` reading the point-read column resolves to that row's own vector.
WITH (SELECT vec FROM quantize_pr_default_from_vec WHERE id = 1000) AS ref
SELECT 'default_from_vector', count() AS n, countIf(norm = vec[1]) AS ok
FROM (SELECT id, norm, vec FROM quantize_pr_default_from_vec ORDER BY L2Distance(vec, ref) ASC LIMIT 50 SETTINGS vector_search_index_fetch_multiplier = 100);

-- Same for `MATERIALIZED`.
WITH (SELECT vec FROM quantize_pr_default_from_vec WHERE id = 1000) AS ref
SELECT 'materialized_from_vector', count() AS n, countIf(norm_mat = vec[2]) AS ok
FROM (SELECT id, norm_mat, vec FROM quantize_pr_default_from_vec ORDER BY L2Distance(vec, ref) ASC LIMIT 50 SETTINGS vector_search_index_fetch_multiplier = 100);

DROP TABLE quantize_pr_default_from_vec;
