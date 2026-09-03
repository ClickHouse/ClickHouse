-- Tags: no-parallel-replicas, no-replicated-database
-- The read-time top-k path (use_skip_indexes_for_top_k=1 + use_skip_indexes_on_data_read=1,
-- both on by default) reads a part's minmax into threshold_tracker and drops granules whose
-- advertised extreme falls outside the running threshold. A pending ALTER that touches the
-- indexed column leaves the part's minmax stale: it under-advertises the values a read of
-- the column actually returns, so the granule holding the live top rows gets silently
-- dropped once another part has established the threshold. The per-part staleness check
-- (canUseIndex) must gate the read-time minmax build as well, the same way it already
-- gates the upfront top-k path (partHasStaleTopKIndex) and the regular skip-index path.
--
-- The DROP+RENAME case below uses the two-command form `(DROP COLUMN x), (RENAME COLUMN
-- y TO x)` rather than the single-statement `DROP COLUMN x, RENAME COLUMN y TO x` used
-- in 05045/05055: the parenthesised form records the pair as two independent mutation
-- commands, which is the shape that exercises this gate.
--
-- Every scenario only reads while the ALTER mutation is still pending (`SYSTEM STOP
-- MERGES` keeps it from materializing); that is the state that exercises the gate.
-- Materializing the mutation afterwards is intentionally NOT asserted here: applying a
-- pending DROP/RENAME/ADD while the part is being merged materializes it wrongly under a
-- separate upstream bug, so asserting the materialized result would be flaky until that
-- bug is fixed.

SET query_plan_max_limit_for_top_k_optimization = 1000;

-- Pending ALTER MODIFY COLUMN changes the indexed column's type: the on-disk minmax still
-- holds bytes serialized with the old type. The decoy part's UInt64 value 13830554455654793216
-- casts to the Float64 max 1.38e19, but its raw bytes read as Float64 give -1.0, which looks
-- minimal. Sanity check: gating must NOT change behaviour here — the upfront gate already
-- excludes this part from top-k pre-selection, so the read-time gate must agree.
DROP TABLE IF EXISTS topk_modify;
CREATE TABLE topk_modify (c0 UInt64, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES topk_modify;
INSERT INTO topk_modify SELECT toUInt64(5) FROM numbers(50000) SETTINGS max_insert_threads = 1;
INSERT INTO topk_modify VALUES (13830554455654793216);
ALTER TABLE topk_modify MODIFY COLUMN c0 Float64 SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'modify no-opt', c0 FROM topk_modify ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes = 0;
SELECT 'modify read-time', c0 FROM topk_modify ORDER BY c0 DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1, max_threads = 1;
DROP TABLE topk_modify;

-- Pending DROP COLUMN + ADD COLUMN with a DEFAULT: reads of the decoy part return the new
-- default 1000000, but the part's minmax still advertises the old value 0. Sanity check:
-- like MODIFY above, the upfront gate already excludes this part, so the read-time gate
-- must also leave the result unchanged.
DROP TABLE IF EXISTS topk_drop_add;
CREATE TABLE topk_drop_add (c0 Int32, pad Int32, INDEX idx_c0 c0 TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES topk_drop_add;
INSERT INTO topk_drop_add SELECT toInt32(number) + 1, 0 FROM numbers(50000) SETTINGS max_insert_threads = 1;
INSERT INTO topk_drop_add VALUES (0, 0);
ALTER TABLE topk_drop_add (DROP COLUMN c0), (ADD COLUMN c0 Int32 DEFAULT 1000000) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'drop-add no-opt', c0 FROM topk_drop_add ORDER BY c0 DESC LIMIT 1 SETTINGS use_skip_indexes = 0;
SELECT 'drop-add read-time', c0 FROM topk_drop_add ORDER BY c0 DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1, max_threads = 1;
DROP TABLE topk_drop_add;

-- Pending DROP COLUMN + RENAME COLUMN into the freed name, written as two separate
-- commands. Reads of the decoy part return the renamed y column's data (1000000), but
-- the part's minmax file for the name x still holds the old x column's [0,0]. Without
-- the read-time gate, the decoy granule is dropped against the stale minmax 0 once the
-- live part (x=5) establishes the threshold 5, and the query silently returns 5.
DROP TABLE IF EXISTS topk_rename;
CREATE TABLE topk_rename (x Int32, y Int32, INDEX idx_x x TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES topk_rename;
INSERT INTO topk_rename SELECT 5, 5 FROM numbers(50000) SETTINGS max_insert_threads = 1;
INSERT INTO topk_rename VALUES (0, 1000000);
ALTER TABLE topk_rename (DROP COLUMN x), (RENAME COLUMN y TO x) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'rename no-opt', x FROM topk_rename ORDER BY x DESC LIMIT 1 SETTINGS use_skip_indexes = 0;
SELECT 'rename read-time', x FROM topk_rename ORDER BY x DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1, max_threads = 1;
DROP TABLE topk_rename;
