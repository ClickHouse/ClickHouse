-- Tags: no-parallel-replicas
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
