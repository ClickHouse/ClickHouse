-- Tags: no-parallel-replicas
-- Point-read rescore on a table holding both a Wide (point-readable) and a Compact part. The point read serves the
-- parts it can and leaves the rest to the granule read, so a shortlist spanning both must still be exactly correct.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_pr_mixed_aligned;
DROP TABLE IF EXISTS quantize_pr_mixed_unaligned;

CREATE TABLE quantize_pr_mixed_aligned
(
    id UInt32,
    payload String,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 100000, min_rows_for_wide_part = 1000;

CREATE TABLE quantize_pr_mixed_unaligned
(
    id UInt32,
    payload String,
    vec Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 100000, min_rows_for_wide_part = 1000;

-- A merge would fuse the two parts into one Wide part and destroy the mixed layout this test is about.
SYSTEM STOP MERGES quantize_pr_mixed_aligned;
SYSTEM STOP MERGES quantize_pr_mixed_unaligned;

-- The big insert clears both wide-part thresholds, the small one clears neither: one Wide part and one Compact part.
-- `id` seeds every vector, so all 5100 are distinct and the neighbour order is unambiguous.
INSERT INTO quantize_pr_mixed_aligned
SELECT number, concat('p_', toString(number)),
    arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(5000);
INSERT INTO quantize_pr_mixed_aligned
SELECT number + 100000, concat('p_', toString(number + 100000)),
    arrayMap(j -> toFloat32(sipHash64(number + 100000, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(100);

INSERT INTO quantize_pr_mixed_unaligned SELECT * FROM quantize_pr_mixed_aligned WHERE id < 100000;
INSERT INTO quantize_pr_mixed_unaligned SELECT * FROM quantize_pr_mixed_aligned WHERE id >= 100000;

SELECT 'mixed_part_types',
    (SELECT groupArray(part_type) FROM (SELECT part_type FROM system.parts
        WHERE database = currentDatabase() AND table = 'quantize_pr_mixed_aligned' AND active ORDER BY name)) = ['Wide', 'Compact'];

-- The reference vector belongs to the Compact part, so the shortlist is drawn from both parts at once.
WITH (SELECT vec FROM quantize_pr_mixed_aligned WHERE id = 100050) AS ref
SELECT 'mixed_shortlist_spans_both_parts',
    countIf(id >= 100000) > 0 AND countIf(id < 100000) > 0
FROM (SELECT id FROM quantize_pr_mixed_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 300);

-- Mixed (point read + granule read) must return exactly what the all-granule table returns, payload included.
WITH (SELECT vec FROM quantize_pr_mixed_aligned WHERE id = 100050) AS ref
SELECT 'mixed_aligned_eq_unaligned',
    (SELECT arraySort(groupArray((id, payload))) FROM (SELECT id, payload FROM quantize_pr_mixed_aligned   ORDER BY L2Distance(vec, ref) ASC LIMIT 300))
    = (SELECT arraySort(groupArray((id, payload))) FROM (SELECT id, payload FROM quantize_pr_mixed_unaligned ORDER BY L2Distance(vec, ref) ASC LIMIT 300));

-- Each rescored row keeps its own payload: a wrong interleave of the two kinds of source would shift it.
WITH (SELECT vec FROM quantize_pr_mixed_aligned WHERE id = 100050) AS ref
SELECT 'mixed_payload_belongs_to_row', countIf(payload != concat('p_', toString(id))) = 0
FROM (SELECT id, payload FROM quantize_pr_mixed_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 300);

-- A row stored in the Compact part is still reachable through the merged pipeline.
WITH (SELECT vec FROM quantize_pr_mixed_aligned WHERE id = 100050) AS ref
SELECT 'mixed_compact_row_is_self',
    (SELECT id FROM quantize_pr_mixed_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 100050;

-- Whole-table shortlist: every row of both parts comes back, each with its own payload.
WITH (SELECT vec FROM quantize_pr_mixed_aligned WHERE id = 2500) AS ref
SELECT 'mixed_all_rows_intact', count() = 5100 AND countIf(payload != concat('p_', toString(id))) = 0
FROM (SELECT id, payload FROM quantize_pr_mixed_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 5100);

DROP TABLE quantize_pr_mixed_aligned;
DROP TABLE quantize_pr_mixed_unaligned;
