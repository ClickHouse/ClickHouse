-- Tags: no-random-merge-tree-settings, no-old-analyzer
-- no-old-analyzer: lazy materialization is only available with the analyzer.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET allow_experimental_parallel_reading_from_replicas = 0;
SET distributed_push_down_limit = 1;
SET exact_rows_before_limit = 0;
SET output_format_write_statistics = 0;
SET format_template_row_format = '${0:Raw}';
SET format_template_resultset_format = '${data}{"rows_before_limit_at_least":${rows_before_limit:Raw}}\n';
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET query_plan_max_limit_for_top_k_optimization = 100;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 1;

DROP VIEW IF EXISTS preferred_local_prelimit_view;
DROP TABLE IF EXISTS preferred_local_prelimit;
CREATE TABLE preferred_local_prelimit
(
    id UInt64,
    sort_key UInt64,
    payload String,
    extra String
)
ENGINE = MergeTree
ORDER BY id;

-- Enough rows to exercise more than one read block.
INSERT INTO preferred_local_prelimit
SELECT
    number,
    cityHash64(number),
    concat('payload-', toString(number)),
    concat('extra-', toString(number))
FROM numbers(20000);

-- In-process shard plans must retain the preliminary LIMIT used by both optimizations.
SET prefer_localhost_replica = 1;
SELECT
    countIf(explain LIKE '%preliminary LIMIT%') > 0,
    countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0,
    countIf(explain LIKE '%__topKFilter(sort_key)%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT payload, extra, sort_key
    FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
    ORDER BY sort_key DESC
    LIMIT 10
);

SELECT
    countIf(explain LIKE '%preliminary LIMIT%') > 0,
    countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0,
    countIf(explain LIKE '%__topKFilter(sort_key)%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT payload, extra, sort_key
    FROM preferred_local_prelimit
    ORDER BY sort_key DESC
    LIMIT 10
    SETTINGS
        automatic_parallel_replicas_mode = 0,
        enable_parallel_replicas = 1,
        max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_non_replicated_merge_tree = 1,
        parallel_replicas_local_plan = 1
);

-- Exact accounting must not skip source rows with TopK filtering.
SET exact_rows_before_limit = 1;
SELECT countIf(explain LIKE '%__topKFilter(sort_key)%') = 0
FROM
(
    EXPLAIN actions = 1
    SELECT payload, extra, sort_key
    FROM preferred_local_prelimit
    ORDER BY sort_key DESC
    LIMIT 10
);

-- There are 30 qualifying rows on each shard, so the exact count is 60.
SET prefer_localhost_replica = 0;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template;
SET prefer_localhost_replica = 1;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET 3 FORMAT Template;

SET prefer_localhost_replica = 0;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET -3 FORMAT Template;
SET prefer_localhost_replica = 1;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
WHERE id < 30 ORDER BY id LIMIT 1 OFFSET -3 FORMAT Template;

SET prefer_localhost_replica = 0;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
WHERE id < 30 ORDER BY id LIMIT -1 OFFSET 3 FORMAT Template;
SET prefer_localhost_replica = 1;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
WHERE id < 30 ORDER BY id LIMIT -1 OFFSET 3 FORMAT Template;

SET prefer_localhost_replica = 0;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
WHERE id < 30 ORDER BY id LIMIT -1 OFFSET -3 FORMAT Template;
SET prefer_localhost_replica = 1;
SELECT '' FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
WHERE id < 30 ORDER BY id LIMIT -1 OFFSET -3 FORMAT Template;

-- A LIMIT inside the query remains an accounting boundary.
SELECT ''
FROM
(
    SELECT id
    FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit)
    WHERE id < 30
    ORDER BY id
    LIMIT 17
)
LIMIT 1
FORMAT Template;

SELECT ''
FROM
(
    SELECT id
    FROM preferred_local_prelimit
    WHERE id < 30
    ORDER BY id
    LIMIT 17
)
LIMIT 1
FORMAT Template;

-- A view applies its LIMIT once per shard, so the coordinator receives 34 rows.
CREATE VIEW preferred_local_prelimit_view AS
SELECT id
FROM preferred_local_prelimit
WHERE id < 30
ORDER BY id
LIMIT 17;
SELECT ''
FROM cluster(test_cluster_two_shards, currentDatabase(), preferred_local_prelimit_view)
LIMIT 1
FORMAT Template;

DROP VIEW preferred_local_prelimit_view;
DROP TABLE preferred_local_prelimit;
