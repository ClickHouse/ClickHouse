-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- no-fasttest: a UNIQUE KEY insert writes the dense-index SST, which needs RocksDB
-- no-object-storage, no-s3-storage: UNIQUE KEY requires a storage policy of local disks
-- no-ordinary-database, no-replicated-database, no-shared-merge-tree: UNIQUE KEY is only supported
--                       on plain MergeTree in an Atomic database

-- The range row count of a UNIQUE KEY table also counts the versions its delete bitmap hides at read
-- time, so it must not count as a lower bound when the join build side is chosen. Companion of
-- 04726_join_build_side_row_upper_bound, whose other guard arms cannot carry these tags.

SET enable_unique_key = 1;
SET async_insert = 0;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_merge_expression_into_join = 1;
SET enable_join_runtime_filters = 0;
SET join_algorithm = 'hash';
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS lhs_04726;
DROP TABLE IF EXISTS uk_04726;

CREATE TABLE lhs_04726 (id Int32, pad Int32) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '';
INSERT INTO lhs_04726 VALUES (0, 1), (1, 1);

-- Every INSERT overwrites the same key: 10 rows in the selected ranges, 1 live row.
CREATE TABLE uk_04726 (id Int32, pad Int32) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
    SETTINGS auto_statistics_types = '';
SYSTEM STOP MERGES uk_04726;
INSERT INTO uk_04726 VALUES (0, 1);
INSERT INTO uk_04726 VALUES (0, 2);
INSERT INTO uk_04726 VALUES (0, 3);
INSERT INTO uk_04726 VALUES (0, 4);
INSERT INTO uk_04726 VALUES (0, 5);
INSERT INTO uk_04726 VALUES (0, 6);
INSERT INTO uk_04726 VALUES (0, 7);
INSERT INTO uk_04726 VALUES (0, 8);
INSERT INTO uk_04726 VALUES (0, 9);
INSERT INTO uk_04726 VALUES (0, 10);

SELECT 'live rows', count() FROM uk_04726;
SELECT 'range rows', sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'uk_04726' AND active;

-- `pad` is not in any index, so the left side has only its upper bound of 2 rows. Trusting the 10
-- range rows of the right side as a lower bound would put the 2-row left side on the build side.
SELECT 'UNIQUE KEY right side keeps orientation',
        countIf(explain ILIKE '%uk\_04726%') > 0
    AND countIf(explain ILIKE '%Join: uk\_04726%') = 0 FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM lhs_04726 JOIN uk_04726 ON lhs_04726.id = uk_04726.id
    WHERE lhs_04726.pad = 1
) WHERE explain ILIKE '%Join:%';

SELECT 'rows', count(), sum(uk_04726.pad) FROM lhs_04726 JOIN uk_04726 ON lhs_04726.id = uk_04726.id
WHERE lhs_04726.pad = 1;

DROP TABLE lhs_04726;
DROP TABLE uk_04726;
