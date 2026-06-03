-- Test projection rebuild during a merge that applies lightweight deletes (_row_exists)
-- Tags: no-random-merge-tree-settings
SET lightweight_deletes_sync = 2, mutations_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS t_lwd_merge_proj;

CREATE TABLE t_lwd_merge_proj
(
    id UInt64,
    val UInt64,
    PROJECTION p (SELECT val, count() GROUP BY val)
) ENGINE = MergeTree ORDER BY id
SETTINGS lightweight_mutation_projection_mode = 'rebuild', min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_merge_proj SELECT number, number % 3 FROM numbers(100);
INSERT INTO t_lwd_merge_proj SELECT number + 100, number % 3 FROM numbers(100);

DELETE FROM t_lwd_merge_proj WHERE id % 2 = 0;

-- Merges two parts that both carry _row_exists masks: projection is rebuilt during the merge.
OPTIMIZE TABLE t_lwd_merge_proj FINAL;

SELECT 'via_projection';
SELECT val, count() FROM t_lwd_merge_proj GROUP BY val ORDER BY val
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'ground_truth';
SELECT val, count() FROM t_lwd_merge_proj GROUP BY val ORDER BY val
SETTINGS optimize_use_projections = 0;

DROP TABLE t_lwd_merge_proj;
