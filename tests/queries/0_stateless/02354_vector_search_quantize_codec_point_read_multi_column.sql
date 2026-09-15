-- Tags: no-parallel-replicas
-- Point-read rescore when the lazy read carries more than the vector column. The `Quantized`-codec vector search
-- point-reads the block-aligned vector column, while any other lazily materialized column is read with an ordinary
-- reader for the same row offsets and merged into the same chunk. Two things are checked here:
--   * the row -> value mapping of those other columns survives the merge, including when several shortlisted rows
--     fall into the same granule (they are then read by continuing forward through the granule rather than by
--     re-seeking to its mark, so an off-by-one in the skipped-row accounting would shift the payload);
--   * a lazy read holding two quantized vector columns still returns correct results (the step cannot tell which one
--     the search ranks by, so it declines the fast path and reads everything by granule).
-- Vectors are correlated with the sort key on purpose, so the shortlist concentrates in few granules.
-- Note: `ORDER BY` must hold the distance and nothing else, otherwise the two-stage rewrite does not engage at all.

SET allow_experimental_codecs = 1;
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

-- A shortlist packed into a single granule: every candidate is read from the same granule of `payload`, which is read
-- once by continuing forward through it rather than once per candidate.
WITH (SELECT vec FROM quantize_pr_mc_aligned WHERE id = 300) AS ref
SELECT 'dense_granule_payload_matches_row', countIf(payload = repeat(concat('p', toString(id), '_'), 8)), count()
FROM (SELECT id, payload FROM quantize_pr_mc_aligned WHERE id < 512 ORDER BY L2Distance(vec, ref) ASC LIMIT 200 SETTINGS vector_search_index_fetch_multiplier = 20);

-- Same, but with a granule that spans many compressed blocks of the payload column (8192 rows of ~2 KB against a
-- 64 KB minimum block). Re-seeking to the mark per candidate would decompress the blocks between the mark and the
-- candidate again for every one of them, which costs orders of magnitude more than the granule itself; reading
-- forward touches each block once. Wrong skip accounting shows up as a mismatched payload.
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

-- The point read addresses the part's files directly, so it must decline any part that needs a read-time conversion
-- or that does not store one of the lazy columns. Both cases below return correct results only on the granule read.

-- A pending `DROP COLUMN` is metadata-only: the part keeps the old data and the reader is supposed to ignore it. If
-- the point read served this part it would hand back the pre-drop bytes of the re-added column.
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

-- A column added after the part was written is absent from it and has to be synthesized from its `DEFAULT`. Here it is
-- the only lazy column besides the vector one, so nothing else carries the row count into the default evaluation.
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
