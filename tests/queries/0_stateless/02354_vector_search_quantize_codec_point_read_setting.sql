-- Tags: no-parallel-replicas
-- `quantized_vector_one_block_per_row` gates both halves of the point read: the writer's one-vector-per-block layout
-- and the reader's positional path. It is alterable, so the two can be flipped over existing data in either order.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

SELECT 'default_is_off', value FROM system.merge_tree_settings WHERE name = 'quantized_vector_one_block_per_row';

DROP TABLE IF EXISTS quantize_pr_gate;

CREATE TABLE quantize_pr_gate (id UInt32, payload String, vec Array(Float32) CODEC(Quantized('int8', 64)))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- A merge would fuse the two layouts into one part and destroy the mixed state this test is about.
SYSTEM STOP MERGES quantize_pr_gate;

-- Part written with the setting off, then the setting is turned on and a second part is written: one table, two layouts.
INSERT INTO quantize_pr_gate
SELECT number, concat('p_', toString(number)),
    arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(3000);

ALTER TABLE quantize_pr_gate MODIFY SETTING quantized_vector_one_block_per_row = 1;

INSERT INTO quantize_pr_gate
SELECT number + 100000, concat('p_', toString(number + 100000)),
    arrayMap(j -> toFloat32(sipHash64(number + 100000, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(3000);

-- Both parts hold 3000 rows, but one vector per block keeps the elements uncompressed and adds 25 bytes of framing
-- per row, so the part written with the setting on is the larger one. (`column_bytes_on_disk` covers every substream
-- of `vec`, not just the elements, which is why this compares the two parts rather than an absolute size.)
SELECT 'aligned_part_is_larger',
    (SELECT c.column_bytes_on_disk FROM system.parts AS p
     INNER JOIN system.parts_columns AS c ON c.database = p.database AND c.table = p.table AND c.name = p.name
     WHERE p.database = currentDatabase() AND p.table = 'quantize_pr_gate' AND p.active AND c.column = 'vec' AND p.name = 'all_2_2_0')
  > (SELECT c.column_bytes_on_disk FROM system.parts AS p
     INNER JOIN system.parts_columns AS c ON c.database = p.database AND c.table = p.table AND c.name = p.name
     WHERE p.database = currentDatabase() AND p.table = 'quantize_pr_gate' AND p.active AND c.column = 'vec' AND p.name = 'all_1_1_0');

-- A shortlist spanning both layouts is served correctly, with each row keeping its own payload.
WITH (SELECT vec FROM quantize_pr_gate WHERE id = 100050) AS ref
SELECT 'mixed_layout_payload_ok', count() AS n, countIf(payload != concat('p_', toString(id))) = 0 AS ok
FROM (SELECT id, payload FROM quantize_pr_gate ORDER BY L2Distance(vec, ref) ASC LIMIT 200);

-- Turning it back off must not disturb the part that is already aligned on disk: it just stops being point-read.
ALTER TABLE quantize_pr_gate MODIFY SETTING quantized_vector_one_block_per_row = 0;

WITH (SELECT vec FROM quantize_pr_gate WHERE id = 100050) AS ref
SELECT 'reads_aligned_part_with_setting_off', count() AS n, countIf(payload != concat('p_', toString(id))) = 0 AS ok
FROM (SELECT id, payload FROM quantize_pr_gate ORDER BY L2Distance(vec, ref) ASC LIMIT 200);

-- And the answer is the same one the plain granule read gives.
WITH (SELECT vec FROM quantize_pr_gate WHERE id = 100050) AS ref
SELECT 'matches_granule_read',
    (SELECT arraySort(groupArray((id, payload))) FROM (SELECT id, payload FROM quantize_pr_gate ORDER BY L2Distance(vec, ref) ASC LIMIT 200))
    = (SELECT arraySort(groupArray((id, payload))) FROM (SELECT id, payload FROM quantize_pr_gate ORDER BY L2Distance(vec, ref) ASC LIMIT 200 SETTINGS query_plan_optimize_lazy_materialization = 0));

DROP TABLE quantize_pr_gate;
