-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-fasttest
-- UNIQUE KEY reads after a DELETE: every read path honours the delete bitmap.
--   1. count(): with the implicit minmax-count projection and the sparsity-filter rewrite on
--   2. granule skip: a fully dead granule is skipped, the live one after it is not
--   3. PREWHERE on `_part_offset`: the bitmap still finds its offsets
--   4. lazy materialization: the bitmap is not applied twice
--   5. text index: a hasToken read honours the bitmap
--   6. make_distributed_plan: the read falls back to local execution, which applies the bitmap

SET enable_unique_key = 1;

-- 1. count(): red if the implicit minmax-count projection serves a UNIQUE KEY table
-- (`count_pred_implicit_proj` 5), the defaultness stats answer for one (`plan_pred_sparsity` 1),
-- or the trivial count counts dead rows (`count_implicit_proj` 5).
DROP TABLE IF EXISTS uk_guard;

CREATE TABLE uk_guard (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
-- Pin what the sparsity-filter rewrite needs, or randomized settings decide whether it runs.
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

SYSTEM STOP MERGES uk_guard;

INSERT INTO uk_guard VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

DELETE FROM uk_guard WHERE id % 2 = 0;  -- removes 2, 4

SET optimize_use_implicit_projections = 1;
SET optimize_trivial_count_query = 1;
SELECT 'count_implicit_proj' AS step, count() FROM uk_guard;  -- 3
SELECT 'count_pred_implicit_proj' AS step, count() FROM uk_guard WHERE id >= 1;  -- 3

SELECT 'plan_pred_sparsity' AS step, countIf(explain LIKE '%Optimized trivial count with sparsity filter%')
FROM (EXPLAIN SELECT count() FROM uk_guard WHERE id >= 1 SETTINGS optimize_trivial_count_with_sparsity_filter = 1)
SETTINGS enable_analyzer = 1;  -- 0

-- Control: the same table without UNIQUE KEY still gets the rewrite.
DROP TABLE IF EXISTS no_uk_guard;
CREATE TABLE no_uk_guard (id UInt64, v String) ENGINE = MergeTree ORDER BY (id)
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;
INSERT INTO no_uk_guard VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
SELECT 'plan_pred_sparsity_no_uk' AS step, countIf(explain LIKE '%Optimized trivial count with sparsity filter%')
FROM (EXPLAIN SELECT count() FROM no_uk_guard WHERE id >= 1 SETTINGS optimize_trivial_count_with_sparsity_filter = 1)
SETTINGS enable_analyzer = 1;  -- 1
DROP TABLE no_uk_guard;

DROP TABLE uk_guard;

-- 2. granule skip: red if the granule skip judges a mark by the previous one's rows
-- (`gran_after_delete` 0).
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS uk_granule;

CREATE TABLE uk_granule (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_granule;

INSERT INTO uk_granule VALUES
    (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'),
    (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h');

SELECT 'gran_after_insert' AS step, count() FROM uk_granule;  -- 8

DELETE FROM uk_granule WHERE id <= 4;

SELECT 'gran_after_delete' AS step, count() FROM uk_granule;  -- 4
SELECT 'gran_survivors' AS step, id, v FROM uk_granule ORDER BY id;  -- 5..8
SELECT 'gran_filtered' AS step, count() FROM uk_granule WHERE id < 6;  -- only 5

DROP TABLE uk_granule;

-- 3. PREWHERE: red if the reader stops keeping `_part_offset` after PREWHERE consumes it
-- (NOT_FOUND_COLUMN_IN_BLOCK).

DROP TABLE IF EXISTS uk_po_prewhere;

CREATE TABLE uk_po_prewhere (id UInt64, grp UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_po_prewhere;

INSERT INTO uk_po_prewhere VALUES
    (1, 100, 'a'), (2, 100, 'b'), (3, 200, 'c'),
    (4, 200, 'd'), (5, 100, 'e'), (6, 200, 'f');

DELETE FROM uk_po_prewhere WHERE id IN (2, 4);
SELECT 'po_after_delete' AS step, count() FROM uk_po_prewhere;  -- 4

SELECT 'po_all_rows' AS step, id, v FROM uk_po_prewhere PREWHERE _part_offset >= 0 ORDER BY id;  -- 1 3 5 6

SELECT 'po_grp100' AS step, id, v FROM uk_po_prewhere PREWHERE _part_offset >= 0 AND grp = 100 ORDER BY id;  -- 1 a / 5 e

DROP TABLE uk_po_prewhere;

-- 4. lazy: red if the lazy-materialization step also gets the bitmaps and applies them a second
-- time (LOGICAL_ERROR).
DROP TABLE IF EXISTS uk_po_lazy;

CREATE TABLE uk_po_lazy (id UInt32, v UInt32, s String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_po_lazy;

INSERT INTO uk_po_lazy SELECT number, 1000 - number, concat('x', toString(number)) FROM numbers(100);
INSERT INTO uk_po_lazy SELECT number + 100, 800 - number, concat('y', toString(number)) FROM numbers(50);
DELETE FROM uk_po_lazy WHERE id IN (99, 98, 97, 50, 149, 148);
INSERT INTO uk_po_lazy VALUES (96, 1, 'replaced96');

SELECT 'lazy' AS step, id, v, s FROM uk_po_lazy ORDER BY v LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;

DROP TABLE uk_po_lazy;

-- 5. text index: red if a text-index read skips the bitmap (`token_search` 2).
-- Force the text-index direct-read path; the runner randomizes both settings.
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS uk_txt;

CREATE TABLE uk_txt
(
    id UInt64,
    txt String,
    INDEX txt_idx txt TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_txt;

INSERT INTO uk_txt VALUES (1, 'alpha beta'), (2, 'alpha gamma'), (3, 'alpha epsilon');

DELETE FROM uk_txt WHERE id = 2;

SELECT 'token_search' AS step, id FROM uk_txt WHERE hasToken(txt, 'alpha') ORDER BY id;  -- 1,3
SELECT 'plain_scan' AS step, id FROM uk_txt ORDER BY id;  -- 1,3

DROP TABLE uk_txt;

-- 6. distributed plan: red if planning accepts a UNIQUE KEY read for a distributed plan
-- (`dplan_fallback` fails serializing the plan, and the EXPLAIN runs instead of refusing at planning).
DROP TABLE IF EXISTS uk_dplan;

CREATE TABLE uk_dplan (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

INSERT INTO uk_dplan VALUES (1, 'a'), (2, 'b'), (3, 'c');
DELETE FROM uk_dplan WHERE id = 2;

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SELECT 'dplan_fallback' AS step, id, v FROM uk_dplan ORDER BY id
SETTINGS distributed_plan_fallback_to_local_execution = 1;  -- 1 a / 3 c
EXPLAIN PIPELINE SELECT id FROM uk_dplan
SETTINGS distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE uk_dplan;
