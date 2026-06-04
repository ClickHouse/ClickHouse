-- Tags: no-fasttest, no-ordinary-database, no-random-merge-tree-settings
--
-- Regression for partial PK + vector search.
-- Before this fix, if PK left only part of the marks in a part, vector index analysis was skipped.
-- Here we check:
-- 1) Case A: `use_skip_indexes = 1` and `use_skip_indexes = 0` return the same ids (single PK slice, clear ANN winners).
-- 2) vector path really runs (`USearchSearchCount > 0`).
-- 3) Cases D/E: partial PK + vector index across skip-index granules (index used, results respect WHERE; Case D also checks row count vs skip=0).
-- 4) Date-range PK filters + extra non-PK filters still work as expected.

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;
SET log_queries = 1;

DROP TABLE IF EXISTS tab_pk_partial;

-- Cases D/E need multiple data marks and skip-index granules. vector_similarity requires index_granularity_bytes != 0;
-- use a large byte limit so 12 rows still split only by index_granularity = 3.
CREATE TABLE tab_pk_partial(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 10485760;

INSERT INTO tab_pk_partial VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]),
    (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

-- Case A: simple PK partial range (`id >= 6`) with vector ORDER BY LIMIT.
-- Expected: same top-k ids with and without skip indexes.
SELECT 'pk_partial_matches_exact_knn_without_skip_indexes';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT
    (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_pk_partial
            WHERE id >= 6
            ORDER BY L2Distance(vec, reference_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 1
        )
    ) = (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_pk_partial
            WHERE id >= 6
            ORDER BY L2Distance(vec, reference_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 0
        )
    );

SELECT id
FROM tab_pk_partial
WHERE id >= 6
ORDER BY L2Distance(vec, [toFloat32(0.), toFloat32(2.)]) ASC
LIMIT 3
SETTINGS use_skip_indexes = 1, log_comment = '04217-vector-index-path'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Case B: prove this query actually used the vector index path.
SELECT 'vector_index_path_used';
SELECT max(ProfileEvents['USearchSearchCount'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04217-vector-index-path'
    AND event_date >= yesterday()
    AND event_time >= now() - 600;

SELECT 'expected_top3_ids_for_reference_vec';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT arraySort(groupArray(id))
FROM
(
    SELECT id
    FROM tab_pk_partial
    WHERE id >= 6
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);

-- Case D: PK spans two skip-index granules (OR). Do not require exact id match vs skip=0 (ANN/bf16).
SELECT id
FROM tab_pk_partial
WHERE (id <= 2) OR (id >= 7)
ORDER BY L2Distance(vec, [toFloat32(0.), toFloat32(2.)]) ASC
LIMIT 3
SETTINGS use_skip_indexes = 1, log_comment = '04217-pk-multi-granule'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'pk_multi_granule_vector_index_used';
SELECT max(ProfileEvents['USearchSearchCount'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04217-pk-multi-granule'
    AND event_date >= yesterday()
    AND event_time >= now() - 600;

SELECT 'pk_multi_granule_same_row_count_as_without_skip_indexes';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT
    (
        SELECT count()
        FROM
        (
            SELECT id
            FROM tab_pk_partial
            WHERE (id <= 2) OR (id >= 7)
            ORDER BY L2Distance(vec, reference_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 1
        )
    ) = (
        SELECT count()
        FROM
        (
            SELECT id
            FROM tab_pk_partial
            WHERE (id <= 2) OR (id >= 7)
            ORDER BY L2Distance(vec, reference_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 0
        )
    );

SELECT 'pk_multi_granule_results_within_pk_filter';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT min((id <= 2) OR (id >= 7))
FROM
(
    SELECT id
    FROM tab_pk_partial
    WHERE (id <= 2) OR (id >= 7)
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);

-- Case E: disjoint PK ranges in one skip-index granule (OR). Same relaxed checks as Case D.
SELECT id
FROM tab_pk_partial
WHERE (id <= 1) OR (id >= 4 AND id <= 5)
ORDER BY L2Distance(vec, [toFloat32(0.), toFloat32(2.)]) ASC
LIMIT 3
SETTINGS use_skip_indexes = 1, log_comment = '04217-pk-disjoint-same-granule'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'pk_disjoint_pk_same_granule_vector_index_used';
SELECT max(ProfileEvents['USearchSearchCount'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04217-pk-disjoint-same-granule'
    AND event_date >= yesterday()
    AND event_time >= now() - 600;

SELECT 'pk_disjoint_pk_same_granule_results_within_pk_filter';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT min((id <= 1) OR (id >= 4 AND id <= 5))
FROM
(
    SELECT id
    FROM tab_pk_partial
    WHERE (id <= 1) OR (id >= 4 AND id <= 5)
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);

SELECT 'empty_pk_filter';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT count()
FROM
(
    SELECT id
    FROM tab_pk_partial
    WHERE id > 100
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
);

SELECT 'full_table_top3_sorted_ids';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT arraySort(groupArray(id))
FROM
(
    SELECT id
    FROM tab_pk_partial
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 0
);

DROP TABLE tab_pk_partial;

-- Case C: composite PK (`ORDER BY (created_date, id)`), Date-range filters, and vector ORDER BY LIMIT.
-- Also checks conjunction with extra non-PK predicates.
DROP TABLE IF EXISTS tab_time_tickets;

CREATE TABLE tab_time_tickets(
    id Int32,
    created_date Date,
    issue_type LowCardinality(String),
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2
) ENGINE = MergeTree ORDER BY (created_date, id) SETTINGS index_granularity = 3, index_granularity_bytes = 10485760;

INSERT INTO tab_time_tickets VALUES
    (1, '2024-01-15', 'network', [0.2, 1.8]), (2, '2024-02-10', 'disk', [0.1, 1.7]), (3, '2024-03-12', 'cpu', [0.0, 1.6]),
    (4, '2024-04-18', 'deploy', [0.0, 1.5]), (5, '2024-05-11', 'network', [0.0, 1.4]), (6, '2024-06-20', 'disk', [0.0, 1.3]),
    (7, '2024-07-08', 'oom', [0.0, 1.0]), (8, '2024-08-14', 'oom', [0.0, 1.1]), (9, '2024-09-03', 'oom', [0.0, 1.2]),
    (10, '2024-12-22', 'linux_vm_crash', [1.0, 0.0]), (11, '2024-12-25', 'linux_vm_crash', [1.1, 0.0]), (12, '2024-12-29', 'linux_vm_crash', [1.2, 0.0]);

SELECT 'time_filtered_vector_search';
WITH [toFloat32(1.), toFloat32(0.)] AS query_vec
SELECT
    (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_time_tickets
            WHERE created_date >= '2024-12-22'
            ORDER BY L2Distance(vec, query_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 1
        )
    ) = (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_time_tickets
            WHERE created_date >= '2024-12-22'
            ORDER BY L2Distance(vec, query_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 0
        )
    );

SELECT 'time_filtered_vector_search_with_additional_filter_linux';
WITH [toFloat32(1.), toFloat32(0.)] AS query_vec
SELECT
    (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_time_tickets
            WHERE created_date >= '2024-12-22' AND issue_type = 'linux_vm_crash'
            ORDER BY L2Distance(vec, query_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 1
        )
    ) = (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_time_tickets
            WHERE created_date >= '2024-12-22' AND issue_type = 'linux_vm_crash'
            ORDER BY L2Distance(vec, query_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 0
        )
    );

SELECT 'time_filtered_vector_search_with_additional_filter_oom';
WITH [toFloat32(0.), toFloat32(1.)] AS query_vec
SELECT
    (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_time_tickets
            WHERE created_date >= '2024-07-01' AND created_date < '2025-01-01' AND issue_type = 'oom'
            ORDER BY L2Distance(vec, query_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 1
        )
    ) = (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_time_tickets
            WHERE created_date >= '2024-07-01' AND created_date < '2025-01-01' AND issue_type = 'oom'
            ORDER BY L2Distance(vec, query_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 0
        )
    );

DROP TABLE tab_time_tickets;
