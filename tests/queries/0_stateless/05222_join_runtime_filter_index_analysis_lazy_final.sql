-- A `FINAL` read refuses the join runtime filters for granule pruning: the merging pipeline has to see
-- every version of a row. The lazy `FINAL` rewrite (`query_plan_optimize_lazy_final`) replaces such a read
-- of a `ReplacingMergeTree` with non-`FINAL` reads: one over the parts whose primary key ranges do not
-- intersect, and one that collects the primary keys of the rows passing the filter from the intersecting
-- parts. Both are ordinary reads that can prune with the runtime filters, so they must take over the keys
-- the replaced read was offered, and the build side must keep tracking the key range for them.

SET explain_query_plan_default = 'legacy'; -- the `Key range tracking` line is printed by the non-pretty EXPLAIN
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_optimize_lazy_final = 1;
SET max_rows_for_lazy_final = 10000000;
SET min_filtered_ratio_for_lazy_final = 0;

DROP TABLE IF EXISTS probe_lazy_final_disjoint;
DROP TABLE IF EXISTS probe_lazy_final_overlapping;
DROP TABLE IF EXISTS build_lazy_final;

CREATE TABLE build_lazy_final (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO build_lazy_final SELECT number * 1000 FROM numbers(10);

-- Two parts with disjoint primary key ranges: no row has more than one version, so the rewrite replaces
-- the `FINAL` read with a single non-`FINAL` one over all parts.
CREATE TABLE probe_lazy_final_disjoint (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 8;
SYSTEM STOP MERGES probe_lazy_final_disjoint;
INSERT INTO probe_lazy_final_disjoint SELECT number, number FROM numbers(5000);
INSERT INTO probe_lazy_final_disjoint SELECT number, number FROM numbers(5000, 5000);

-- Two parts over the same primary key range: every row has two versions, so the rewrite keeps the `FINAL`
-- read for them, fed with the primary keys that a non-`FINAL` read collects under the filter first.
CREATE TABLE probe_lazy_final_overlapping (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 8;
SYSTEM STOP MERGES probe_lazy_final_overlapping;
INSERT INTO probe_lazy_final_overlapping SELECT number, number FROM numbers(10000);
INSERT INTO probe_lazy_final_overlapping SELECT number, number + 1 FROM numbers(10000);

SELECT 'disjoint parts, lazy FINAL: the non-FINAL read prunes';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_lazy_final_disjoint AS p FINAL INNER JOIN build_lazy_final AS b ON p.k = b.k
) WHERE explain LIKE '%Key range tracking%' OR explain LIKE '%InputSelector%' OR explain LIKE '%ReadFromMergeTree%';

SELECT 'overlapping parts, lazy FINAL: the set-building read prunes';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_lazy_final_overlapping AS p FINAL INNER JOIN build_lazy_final AS b ON p.k = b.k
) WHERE explain LIKE '%Key range tracking%' OR explain LIKE '%InputSelector%' OR explain LIKE '%ReadFromMergeTree%';

-- The control: without the rewrite the `FINAL` read stays, and it cannot prune.
SELECT 'disjoint parts, plain FINAL: nothing prunes';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM probe_lazy_final_disjoint AS p FINAL INNER JOIN build_lazy_final AS b ON p.k = b.k
    SETTINGS query_plan_optimize_lazy_final = 0
) WHERE explain LIKE '%Key range tracking%' OR explain LIKE '%InputSelector%' OR explain LIKE '%ReadFromMergeTree%';

-- The rewritten reads really drop granules, and the results do not depend on it: the overlapping table
-- must still return the last version of every row (`v = k + 1`).
SELECT 'results';
SELECT count(), sum(v) FROM probe_lazy_final_disjoint AS p FINAL INNER JOIN build_lazy_final AS b ON p.k = b.k
    SETTINGS log_comment = '05222_lazy_final_disjoint';
SELECT count(), sum(v) FROM probe_lazy_final_overlapping AS p FINAL INNER JOIN build_lazy_final AS b ON p.k = b.k
    SETTINGS log_comment = '05222_lazy_final_overlapping';
SELECT count(), sum(v) FROM probe_lazy_final_disjoint AS p FINAL INNER JOIN build_lazy_final AS b ON p.k = b.k
    SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(v) FROM probe_lazy_final_overlapping AS p FINAL INNER JOIN build_lazy_final AS b ON p.k = b.k
    SETTINGS query_plan_optimize_lazy_final = 0;

SYSTEM FLUSH LOGS query_log;
SELECT 'granules pruned by the rewritten reads';
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05222_lazy_final_disjoint', '05222_lazy_final_overlapping')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE probe_lazy_final_disjoint;
DROP TABLE probe_lazy_final_overlapping;
DROP TABLE build_lazy_final;
