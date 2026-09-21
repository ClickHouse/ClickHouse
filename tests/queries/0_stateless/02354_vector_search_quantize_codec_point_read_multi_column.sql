-- Tags: no-parallel-replicas
-- Point-read rescore when other columns are lazily read too: they are read normally and merged in. Checks their row ->
-- value mapping survives, two vector columns decline the fast path, and `ORDER BY` holds only the distance (else no rewrite).

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_pr_mc_aligned;
DROP TABLE IF EXISTS quantize_pr_mc_unaligned;

CREATE TABLE quantize_pr_mc_aligned
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 256),
    payload String,
    vec2 Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE quantize_pr_mc_unaligned
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)),
    payload String,
    vec2 Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO quantize_pr_mc_aligned
SELECT
    number,
    arrayMap(j -> toFloat32(number / 1000.0 + (sipHash64(number, j) % 100) / 1000.0), range(64)),
    repeat(concat('p', toString(number), '_'), 8),
    arrayMap(j -> toFloat32(sipHash64(j, number) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(5000);
INSERT INTO quantize_pr_mc_unaligned SELECT * FROM quantize_pr_mc_aligned;

SELECT 'wide_parts',
    (SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'quantize_pr_mc_aligned' AND active) = 'Wide';

-- The payload of every rescored row belongs to that row.
WITH (SELECT vec FROM quantize_pr_mc_aligned WHERE id = 2500) AS ref
SELECT 'payload_matches_row', countIf(payload = repeat(concat('p', toString(id), '_'), 8)), count()
FROM (SELECT id, payload FROM quantize_pr_mc_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 100 SETTINGS vector_search_index_fetch_multiplier = 20);

-- Point-read (aligned) and granule-read (unaligned) agree on the whole result, payload included.
WITH (SELECT vec FROM quantize_pr_mc_aligned WHERE id = 2500) AS ref
SELECT 'aligned_eq_unaligned',
    (SELECT arraySort(groupArray((id, payload))) FROM (SELECT id, payload FROM quantize_pr_mc_aligned   ORDER BY L2Distance(vec, ref) ASC LIMIT 100 SETTINGS vector_search_index_fetch_multiplier = 20))
    = (SELECT arraySort(groupArray((id, payload))) FROM (SELECT id, payload FROM quantize_pr_mc_unaligned ORDER BY L2Distance(vec, ref) ASC LIMIT 100 SETTINGS vector_search_index_fetch_multiplier = 20));

-- A shortlist packed into one granule: `payload` is read once by continuing forward, not once per candidate.
WITH (SELECT vec FROM quantize_pr_mc_aligned WHERE id = 300) AS ref
SELECT 'dense_granule_payload_matches_row', countIf(payload = repeat(concat('p', toString(id), '_'), 8)), count()
FROM (SELECT id, payload FROM quantize_pr_mc_aligned WHERE id < 512 ORDER BY L2Distance(vec, ref) ASC LIMIT 200 SETTINGS vector_search_index_fetch_multiplier = 20);

-- Same, with a granule spanning many compressed blocks of `payload` (8192 rows of ~2 KB vs a 64 KB minimum block):
-- re-seeking per candidate would re-decompress the blocks before it. Wrong skip accounting shows as a bad payload.
DROP TABLE IF EXISTS quantize_pr_mc_wide_granule;

CREATE TABLE quantize_pr_mc_wide_granule
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 256),
    payload String
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO quantize_pr_mc_wide_granule
SELECT
    number,
    arrayMap(j -> toFloat32(number / 1000.0 + (sipHash64(number, j) % 100) / 1000.0), range(64)),
    repeat(concat('p', toString(number), '_'), 320)
FROM numbers(8192);

WITH (SELECT vec FROM quantize_pr_mc_wide_granule WHERE id = 4000) AS ref
SELECT 'multi_block_granule_payload_matches_row', countIf(payload = repeat(concat('p', toString(id), '_'), 320)), count()
FROM (SELECT id, payload FROM quantize_pr_mc_wide_granule ORDER BY L2Distance(vec, ref) ASC LIMIT 500 SETTINGS vector_search_index_fetch_multiplier = 20);

DROP TABLE quantize_pr_mc_wide_granule;

-- Second quantized vector column in the lazy read: the fast path is declined, results stay correct.
WITH (SELECT vec FROM quantize_pr_mc_aligned WHERE id = 2500) AS ref
SELECT 'two_vector_columns_eq_unaligned',
    (SELECT arraySort(groupArray((id, vec2))) FROM (SELECT id, vec2 FROM quantize_pr_mc_aligned   ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20))
    = (SELECT arraySort(groupArray((id, vec2))) FROM (SELECT id, vec2 FROM quantize_pr_mc_unaligned ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20));

-- ... and the second vector column also belongs to its row.
WITH (SELECT vec FROM quantize_pr_mc_aligned WHERE id = 2500) AS ref
SELECT 'two_vector_columns_match_row', countIf(vec2 = arrayMap(j -> toFloat32(sipHash64(j, toUInt64(id)) % 2000 / 1000.0 - 1.0), range(64))), count()
FROM (SELECT id, vec2 FROM quantize_pr_mc_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20);

-- The point read must decline a part needing a read-time conversion, or missing one of the lazy columns.

-- A pending `DROP COLUMN` is metadata-only: point-reading the part would return the pre-drop bytes.
DROP TABLE IF EXISTS quantize_pr_mc_dropped;

CREATE TABLE quantize_pr_mc_dropped
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 256),
    payload String
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES quantize_pr_mc_dropped;

INSERT INTO quantize_pr_mc_dropped
SELECT
    number,
    arrayMap(j -> toFloat32(number / 1000.0 + (sipHash64(number, j) % 100) / 1000.0), range(64)),
    concat('OLD', toString(number))
FROM numbers(2000);

ALTER TABLE quantize_pr_mc_dropped DROP COLUMN payload SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE quantize_pr_mc_dropped ADD COLUMN payload String DEFAULT 'NEW' SETTINGS alter_sync = 0, mutations_sync = 0;

WITH (SELECT vec FROM quantize_pr_mc_dropped WHERE id = 1000) AS ref
SELECT 'dropped_and_readded_column', countIf(payload = 'NEW'), countIf(startsWith(payload, 'OLD')), count()
FROM (SELECT id, payload FROM quantize_pr_mc_dropped ORDER BY L2Distance(vec, ref) ASC LIMIT 50 SETTINGS vector_search_index_fetch_multiplier = 20);

DROP TABLE quantize_pr_mc_dropped;

-- A column added after the part was written must be synthesized from its `DEFAULT`; here nothing else carries the row count.
DROP TABLE IF EXISTS quantize_pr_mc_added;

CREATE TABLE quantize_pr_mc_added
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO quantize_pr_mc_added
SELECT number, arrayMap(j -> toFloat32(number / 1000.0 + (sipHash64(number, j) % 100) / 1000.0), range(64))
FROM numbers(2000);

ALTER TABLE quantize_pr_mc_added ADD COLUMN extra String DEFAULT 'zzz';

WITH (SELECT vec FROM quantize_pr_mc_added WHERE id = 1000) AS ref
SELECT 'added_column_default', countIf(extra = 'zzz'), count()
FROM (SELECT extra FROM quantize_pr_mc_added ORDER BY L2Distance(vec, ref) ASC LIMIT 50 SETTINGS vector_search_index_fetch_multiplier = 20);

DROP TABLE quantize_pr_mc_added;

DROP TABLE quantize_pr_mc_aligned;
DROP TABLE quantize_pr_mc_unaligned;
