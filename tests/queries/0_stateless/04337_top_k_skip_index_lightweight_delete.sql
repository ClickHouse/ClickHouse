-- Tags: no-parallel-replicas, no-replicated-database
-- no-replicated-database: lightweight updates add a shard, disturbing the deterministic part layout.
-- The minmax skip index reflects all physical rows including ones hidden by a lightweight
-- delete. The top-k granule optimization (use_skip_indexes_for_top_k) keeps only the globally
-- extreme granules, so a part whose stale minmax advertises the extreme value can displace and
-- prune the part that actually holds the live top rows, yielding empty results.

SET query_plan_max_limit_for_top_k_optimization = 1000;

-- Materialized lightweight delete: the deleted part's stale [0,0] minmax must not prune the live part.
DROP TABLE IF EXISTS topk_lwd;
CREATE TABLE topk_lwd (c0 Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO topk_lwd VALUES (0);
INSERT INTO topk_lwd SELECT toInt32(number - 25000) FROM numbers(10000) SETTINGS max_insert_threads = 1;
DELETE FROM topk_lwd WHERE c0 = 0;

SELECT 'count', count() FROM topk_lwd;
SELECT 'max', max(c0) FROM topk_lwd;
SELECT 'DESC LIMIT 1', c0 FROM topk_lwd ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'DESC LIMIT 1 no-opt', c0 FROM topk_lwd ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 0;
SELECT 'DESC LIMIT 5', c0 FROM topk_lwd ORDER BY c0 DESC LIMIT 5 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'ASC LIMIT 1', c0 FROM topk_lwd ORDER BY c0 ASC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;

DROP TABLE topk_lwd;

-- Pending (on-the-fly) lightweight delete: same correctness requirement before the mutation is materialized.
DROP TABLE IF EXISTS topk_lwd_otf;
CREATE TABLE topk_lwd_otf (c0 Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES topk_lwd_otf;
INSERT INTO topk_lwd_otf VALUES (0);
INSERT INTO topk_lwd_otf SELECT toInt32(number - 25000) FROM numbers(10000) SETTINGS max_insert_threads = 1;
SET mutations_sync = 0;
SET apply_mutations_on_fly = 1;
ALTER TABLE topk_lwd_otf UPDATE _row_exists = 0 WHERE c0 = 0;

SELECT 'otf DESC LIMIT 1', c0 FROM topk_lwd_otf ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'otf DESC LIMIT 1 no-opt', c0 FROM topk_lwd_otf ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 0;

SET apply_mutations_on_fly = 0;
DROP TABLE topk_lwd_otf;

-- A clean part next to a fully-deleted part: live max from the clean part wins.
DROP TABLE IF EXISTS topk_mixed;
CREATE TABLE topk_mixed (c0 Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO topk_mixed SELECT toInt32(number) FROM numbers(50000) SETTINGS max_insert_threads = 1;
INSERT INTO topk_mixed SELECT toInt32(1000000) FROM numbers(5000) SETTINGS max_insert_threads = 1;
DELETE FROM topk_mixed WHERE c0 = 1000000;

SELECT 'mixed count', count() FROM topk_mixed;
SELECT 'mixed DESC LIMIT 1', c0 FROM topk_mixed ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'mixed DESC LIMIT 3', c0 FROM topk_mixed ORDER BY c0 DESC LIMIT 3 SETTINGS use_skip_indexes_for_top_k = 1;

DROP TABLE topk_mixed;

-- A clean table without lightweight deletes must still use the optimization.
DROP TABLE IF EXISTS topk_clean;
CREATE TABLE topk_clean (c0 Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO topk_clean SELECT toInt32(number) FROM numbers(100000) SETTINGS max_insert_threads = 1;

SELECT 'clean DESC LIMIT 1', c0 FROM topk_clean ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'EXPLAIN clean: TopK optimization still active';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT c0 FROM topk_clean ORDER BY c0 DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';

DROP TABLE topk_clean;

-- A lightweight UPDATE / patch part that rewrites the indexed sort column makes the base part's
-- minmax stale in the same way a lightweight delete does: the part still advertises the old extreme
-- value. The optimization must skip such parts too.
SET allow_experimental_lightweight_update = 1;
SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

-- Materialized patch: decoy part advertises max 1000000, but its row was updated down to -1.
-- The live max (50000) lives in the other part and must not be pruned.
DROP TABLE IF EXISTS topk_lwu;
CREATE TABLE topk_lwu (c0 Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
             enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO topk_lwu VALUES (1000000);
INSERT INTO topk_lwu SELECT toInt32(number) FROM numbers(50001) SETTINGS max_insert_threads = 1;
UPDATE topk_lwu SET c0 = -1 WHERE c0 = 1000000;

SELECT 'lwu count', count() FROM topk_lwu;
SELECT 'lwu DESC LIMIT 1', c0 FROM topk_lwu ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'lwu DESC LIMIT 1 no-opt', c0 FROM topk_lwu ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 0;
DROP TABLE topk_lwu;

-- Pending (on-the-fly) ALTER UPDATE of the indexed column: same correctness before materialization.
DROP TABLE IF EXISTS topk_lwu_otf;
CREATE TABLE topk_lwu_otf (c0 Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES topk_lwu_otf;
INSERT INTO topk_lwu_otf VALUES (1000000);
INSERT INTO topk_lwu_otf SELECT toInt32(number) FROM numbers(50001) SETTINGS max_insert_threads = 1;
SET mutations_sync = 0;
SET apply_mutations_on_fly = 1;
ALTER TABLE topk_lwu_otf UPDATE c0 = -1 WHERE c0 = 1000000;

SELECT 'otf-upd DESC LIMIT 1', c0 FROM topk_lwu_otf ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'otf-upd DESC LIMIT 1 no-opt', c0 FROM topk_lwu_otf ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 0;
SET apply_mutations_on_fly = 0;
DROP TABLE topk_lwu_otf;

-- Patch moves the indexed column UP, past a clean part's value (reviewer's data-read scenario,
-- checked with use_skip_indexes_on_data_read = 1). The decoy part's base value is 0, so its stale
-- minmax [0,0] UNDER-advertises the real patched value 1000000. A naive top-k ranking would prune
-- the decoy in favour of the clean part (max 50000) and return 50000 instead of 1000000.
DROP TABLE IF EXISTS topk_lwu_up;
CREATE TABLE topk_lwu_up (c0 Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
             enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO topk_lwu_up VALUES (0);
INSERT INTO topk_lwu_up SELECT toInt32(number) + 1 FROM numbers(50000) SETTINGS max_insert_threads = 1;
UPDATE topk_lwu_up SET c0 = 1000000 WHERE c0 = 0;

SELECT 'lwu-up DESC LIMIT 1', c0 FROM topk_lwu_up ORDER BY c0 DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;
SELECT 'lwu-up DESC LIMIT 1 no-opt', c0 FROM topk_lwu_up ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 0;
DROP TABLE topk_lwu_up;

-- A patch that updates a column OTHER than the indexed sort column must NOT disable the optimization.
DROP TABLE IF EXISTS topk_lwu_other;
CREATE TABLE topk_lwu_other (c0 Int32, x Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
             enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO topk_lwu_other SELECT toInt32(number), 0 FROM numbers(100000) SETTINGS max_insert_threads = 1;
UPDATE topk_lwu_other SET x = 7 WHERE c0 = 0;

SELECT 'other-col DESC LIMIT 1', c0 FROM topk_lwu_other ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'EXPLAIN other-col: TopK optimization still active';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT c0 FROM topk_lwu_other ORDER BY c0 DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';
DROP TABLE topk_lwu_other;

-- A pending ordinary ALTER DELETE hides rows on read but, unlike a lightweight delete, adds nothing
-- to the part's updated columns and leaves no _row_exists, so the part still advertises its stale
-- extreme minmax. The decoy part's [1000000,1000000] must not prune the live part (max 49999); the
-- decoy's only row is then filtered out by the delete, so a naive top-k ranking returns no rows.
DROP TABLE IF EXISTS topk_del_otf;
CREATE TABLE topk_del_otf (c0 Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES topk_del_otf;
INSERT INTO topk_del_otf VALUES (1000000);
INSERT INTO topk_del_otf SELECT toInt32(number) FROM numbers(50000) SETTINGS max_insert_threads = 1;
SET mutations_sync = 0;
SET apply_mutations_on_fly = 1;
ALTER TABLE topk_del_otf DELETE WHERE c0 = 1000000;

SELECT 'del-otf DESC LIMIT 1', c0 FROM topk_del_otf ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'del-otf DESC LIMIT 1 no-opt', c0 FROM topk_del_otf ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 0;
SET apply_mutations_on_fly = 0;
DROP TABLE topk_del_otf;

-- A pending ALTER MODIFY COLUMN changes the indexed column's type, but the on-disk minmax still holds
-- bytes serialized with the old type. The decoy part's UInt64 value 13830554455654793216 casts to the
-- Float64 max 1.38e19, yet its raw bytes read as Float64 give -1.0, which looks minimal. A naive top-k
-- ranking on the stale index prunes the decoy granule in favour of the live part (all 5) and returns 5
-- instead of the true max. The MODIFY column must mark the part's index stale.
DROP TABLE IF EXISTS topk_modify_otf;
CREATE TABLE topk_modify_otf (c0 UInt64, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES topk_modify_otf;
INSERT INTO topk_modify_otf VALUES (13830554455654793216);
INSERT INTO topk_modify_otf SELECT toUInt64(5) FROM numbers(50000) SETTINGS max_insert_threads = 1;
SET apply_mutations_on_fly = 1;
-- alter_sync = 0 keeps the MODIFY unmaterialized (merges are stopped), so it is applied on the fly.
ALTER TABLE topk_modify_otf MODIFY COLUMN c0 Float64 SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'modify-otf DESC LIMIT 1', c0 FROM topk_modify_otf ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'modify-otf DESC LIMIT 1 no-opt', c0 FROM topk_modify_otf ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 0;
SET apply_mutations_on_fly = 0;
DROP TABLE topk_modify_otf;

-- A pending ALTER MODIFY COLUMN on a column OTHER than the indexed one must NOT disable the optimization.
DROP TABLE IF EXISTS topk_modify_other;
CREATE TABLE topk_modify_other (c0 Int32, x UInt64, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES topk_modify_other;
INSERT INTO topk_modify_other SELECT toInt32(number), toUInt64(number) FROM numbers(100000) SETTINGS max_insert_threads = 1;
SET apply_mutations_on_fly = 1;
ALTER TABLE topk_modify_other MODIFY COLUMN x Float64 SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'modify-other DESC LIMIT 1', c0 FROM topk_modify_other ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes_for_top_k = 1;
SELECT 'EXPLAIN modify-other: TopK optimization still active';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT c0 FROM topk_modify_other ORDER BY c0 DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';
SET apply_mutations_on_fly = 0;
DROP TABLE topk_modify_other;
