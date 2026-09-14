-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertions below
--  cannot hold there; the query still returns exact results in that case.)
-- The quantized-codes rewrite reads the `<column>.quantized` companion subcolumn IN ADDITION to the full-precision
-- vector, and relies on the lazy-materialization pass that runs after it to defer the vector to the shortlisted rows.
-- Wherever that deferral is unavailable the rewrite used to fire all the same, so both columns were read for every row
-- and the query read MORE than the exact scan the rewrite replaces (issue #119885). Every arm below drives one way the
-- deferral can be unavailable and asserts that the rewrite leaves the query exact instead, together with in-range
-- controls proving the assertions observe the rewrite rather than the fixture.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
-- Lazy materialization (LazilyReadFromMergeTree) is an analyzer-only plan optimization, so the plan-shape assertions
-- below need the analyzer (the old-analyzer CI config does not produce the lazy read).
SET enable_analyzer = 1;
-- Pin the lazy-materialization settings the test harness randomizes: every assertion here is about whether the
-- shortlist is built, which is exactly what those two settings now decide.
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;
SET vector_search_index_fetch_multiplier = 50;

DROP TABLE IF EXISTS quantize_lazy_off;
CREATE TABLE quantize_lazy_off
(
    id UInt32,
    tag UInt8,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id;

-- Cosine distance to row 0 grows strictly with `id`, so the top-k is unique and needs no ORDER BY tie-break. A
-- tie-break would make the sort description two columns wide, which the rewrite declines outright, and every
-- plan-shape assertion below would then pass vacuously.
INSERT INTO quantize_lazy_off
SELECT number, number % 3, arrayMap(j -> toFloat32(if(j = 0, number + 1, 1)), range(64))
FROM numbers(1000);

-- In-range control: with the deferral available the rewrite engages and the vector is read lazily. Without this arm a
-- mutant that always declines would satisfy every assertion below.
SELECT 'control_rewrite_engages_and_defers',
    countIf(explain ILIKE '%quantized shortlist%') > 0,
    countIf(explain ILIKE '%LazilyReadFromMergeTree%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5
);

-- Lazy materialization turned off: the pass the rewrite depends on does not run at all. This is the setting an older
-- `compatibility` profile reverts (it defaults to false before 25.4), which is the reporter's second trigger.
SELECT 'lazy_materialization_off',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0
);

-- The shortlist itself is too large to be deferred: the shortlist limit cannot go below the rows the final top-k
-- returns, so clamping it to `query_plan_max_limit_for_lazy_materialization` cannot rescue this.
SELECT 'shortlist_above_max_limit_for_lazy_materialization',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5 SETTINGS query_plan_max_limit_for_lazy_materialization = 4
);

-- Control for the arm above: `query_plan_max_limit_for_lazy_materialization = 0` means unbounded, not a cap of zero, so
-- the shortlist is still built and the check must not read it as a bail-out.
SELECT 'shortlist_cap_zero_is_unbounded',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5 SETTINGS query_plan_max_limit_for_lazy_materialization = 0
);

-- A PREWHERE that reads the vector column: its inputs are read for every row before the shortlist limit, so the
-- vector cannot be deferred even though lazy materialization does run.
SELECT 'prewhere_reads_vector',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off PREWHERE notEmpty(vec)
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5 SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1
);

-- Control pinning the arm above to the COLUMN rather than to the presence of a filter: a PREWHERE that does not read
-- the vector leaves it deferrable, and the rewrite must still engage. Implementing the check as "decline under any
-- PREWHERE" would pass the arm above and fail here.
SELECT 'prewhere_not_reading_vector',
    countIf(explain ILIKE '%quantized shortlist%') > 0,
    countIf(explain ILIKE '%LazilyReadFromMergeTree%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off PREWHERE tag = 1
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5 SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1
);

-- A WHERE on the vector column that is kept out of PREWHERE stays a filter step below the shortlist, and its input is
-- read for every row. Both prewhere settings have to be off: either one alone still moves the filter.
SELECT 'where_reads_vector_outside_prewhere',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off WHERE notEmpty(vec)
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0
);

-- A row policy reading the vector column reaches the same force-kept set as a PREWHERE.
CREATE ROW POLICY quantize_lazy_off_policy ON quantize_lazy_off USING notEmpty(vec) TO ALL;

SELECT 'row_policy_reads_vector',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5
);

DROP ROW POLICY quantize_lazy_off_policy ON quantize_lazy_off;

-- The results are unchanged by any of this: a declined rewrite runs the exact scan, which is what the rewrite
-- approximates in the first place. The fixture makes the top-5 the five lowest ids in that order, so each arm is
-- pinned against the answer rather than against another query, and `vector_search_index_fetch_multiplier` is raised so
-- that a shortlist, if one is built after all, covers every row and stays exact too. The rows are printed instead of
-- collected with `groupArray`, whose order over an ordered subquery is not specified once the aggregation runs on
-- several threads (measured: the same five ids came back rotated).
SELECT 'exact_results_lazy_off', id FROM quantize_lazy_off
ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0, vector_search_index_fetch_multiplier = 1000;

SELECT 'exact_results_prewhere_reads_vector', id FROM quantize_lazy_off PREWHERE notEmpty(vec)
ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
LIMIT 5 SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, vector_search_index_fetch_multiplier = 1000;

SELECT 'exact_results_where_reads_vector', id FROM quantize_lazy_off WHERE notEmpty(vec)
ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
LIMIT 5 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, vector_search_index_fetch_multiplier = 1000;

-- The point of the fix, measured: with the deferral unavailable, turning the codes path ON must not read more than
-- leaving it off. The two queries differ ONLY in `vector_search_use_quantized_codes`, so after the fix they run the
-- same plan and the byte counts are equal rather than merely close.
SELECT id FROM quantize_lazy_off
ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0, vector_search_use_quantized_codes = 0,
    log_comment = '02354_lazy_off_exact' FORMAT Null;

SELECT id FROM quantize_lazy_off
ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0, vector_search_use_quantized_codes = 1,
    log_comment = '02354_lazy_off_codes' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH
    (SELECT read_bytes FROM system.query_log
     WHERE current_database = currentDatabase() AND event_date >= yesterday()
       AND type = 'QueryFinish' AND log_comment = '02354_lazy_off_exact'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS exact_scan,
    (SELECT read_bytes FROM system.query_log
     WHERE current_database = currentDatabase() AND event_date >= yesterday()
       AND type = 'QueryFinish' AND log_comment = '02354_lazy_off_codes'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS codes_without_deferral
SELECT 'codes_without_deferral_reads_no_more_than_exact_scan',
    exact_scan > 0 AND codes_without_deferral = exact_scan;

-- The old analyzer produces no lazy read at all, so the rewrite must leave the query exact there too. `enable_analyzer`
-- is set at session level because a nested `SETTINGS enable_analyzer` is rejected inside the `EXPLAIN` subquery.
SET enable_analyzer = 0;

SELECT 'analyzer_off_declines',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
    LIMIT 5
);

-- Measured as above, with the old analyzer as the reason the deferral is unavailable.
SELECT id FROM quantize_lazy_off
ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
LIMIT 5 SETTINGS vector_search_use_quantized_codes = 0,
    log_comment = '02354_analyzer_off_exact' FORMAT Null;

SELECT id FROM quantize_lazy_off
ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off WHERE id = 0)) ASC
LIMIT 5 SETTINGS vector_search_use_quantized_codes = 1,
    log_comment = '02354_analyzer_off_codes' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH
    (SELECT read_bytes FROM system.query_log
     WHERE current_database = currentDatabase() AND event_date >= yesterday()
       AND type = 'QueryFinish' AND log_comment = '02354_analyzer_off_exact'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS exact_scan,
    (SELECT read_bytes FROM system.query_log
     WHERE current_database = currentDatabase() AND event_date >= yesterday()
       AND type = 'QueryFinish' AND log_comment = '02354_analyzer_off_codes'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS codes_without_deferral
SELECT 'analyzer_off_reads_no_more_than_exact_scan',
    exact_scan > 0 AND codes_without_deferral = exact_scan;

SET enable_analyzer = 1;

DROP TABLE quantize_lazy_off SYNC;

-- FINAL on an engine other than ReplacingMergeTree: lazy materialization declines on the read step itself, because the
-- merge cannot run above a deferred read.
DROP TABLE IF EXISTS quantize_lazy_off_final;
CREATE TABLE quantize_lazy_off_final
(
    id UInt32,
    cnt UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = SummingMergeTree ORDER BY id;

INSERT INTO quantize_lazy_off_final
SELECT number, 1, arrayMap(j -> toFloat32(if(j = 0, number + 1, 1)), range(64))
FROM numbers(1000);

SELECT 'final_on_summing_merge_tree',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off_final FINAL
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off_final WHERE id = 0)) ASC
    LIMIT 5
);

-- Control: the same table without FINAL still gets the rewrite, so the arm above is about FINAL and not the engine.
SELECT 'summing_merge_tree_without_final',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off_final
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off_final WHERE id = 0)) ASC
    LIMIT 5
);

DROP TABLE quantize_lazy_off_final SYNC;

-- A sampled read: lazy materialization declines on the read step, because the sample is applied while reading and the
-- ranges captured for the deferred read are not the ones the sample will produce.
DROP TABLE IF EXISTS quantize_lazy_off_sample;
CREATE TABLE quantize_lazy_off_sample
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id SAMPLE BY id;

INSERT INTO quantize_lazy_off_sample
SELECT number, arrayMap(j -> toFloat32(if(j = 0, number + 1, 1)), range(64))
FROM numbers(1000);

SELECT 'sample_declines',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off_sample SAMPLE 0.5
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off_sample WHERE id = 0)) ASC
    LIMIT 5
);

-- Control: the same table without the SAMPLE clause still gets the rewrite, so the arm above is about the clause and not
-- about the table having a sampling key.
SELECT 'sample_table_without_sample_clause',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off_sample
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off_sample WHERE id = 0)) ASC
    LIMIT 5
);

DROP TABLE quantize_lazy_off_sample SYNC;

-- Patch parts, the trigger the issue was reported with: one lightweight UPDATE of an unrelated column is enough to
-- disable lazy materialization for the whole table.
SET allow_experimental_lightweight_update = 1;

DROP TABLE IF EXISTS quantize_lazy_off_patch;
CREATE TABLE quantize_lazy_off_patch
(
    id UInt32,
    tag UInt8,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO quantize_lazy_off_patch
SELECT number, number % 3, arrayMap(j -> toFloat32(if(j = 0, number + 1, 1)), range(64))
FROM numbers(1000);

-- Control first: before the UPDATE there is no patch part and the rewrite engages.
SELECT 'patch_parts_control_before_update',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off_patch
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off_patch WHERE id = 0)) ASC
    LIMIT 5
);

UPDATE quantize_lazy_off_patch SET tag = 7 WHERE id = 3;

SELECT 'patch_parts_after_update',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_lazy_off_patch
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_lazy_off_patch WHERE id = 0)) ASC
    LIMIT 5
);

DROP TABLE quantize_lazy_off_patch SYNC;
