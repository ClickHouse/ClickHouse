-- Tags: no-fasttest, no-ordinary-database, no-random-merge-tree-settings
--
-- Partial PK pruning used to skip the vector similarity index; keep using it via filtered_search.

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;
SET log_queries = 1;
-- Most cases below assert that the vector index is used for a partially pruned part, no matter how small the surviving
-- slice is, so disable the selectivity threshold. The default threshold itself is covered by the last case.
SET vector_search_min_surviving_pk_fraction = 0;

DROP TABLE IF EXISTS tab_pk_partial;

-- Cases D/E need multiple data marks and skip-index granules. vector_similarity requires index_granularity_bytes != 0;
-- use a large byte limit so 12 rows still split only by index_granularity = 3.
CREATE TABLE tab_pk_partial(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 10485760;

INSERT INTO tab_pk_partial VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]),
    (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

-- Case A: partial PK range with vector ORDER BY LIMIT.
-- Compare the ordered ids (not just the set): a wrong row-to-distance mapping in the
-- filtered_search path would keep the same candidate set but reorder it, so assert the order.
SELECT 'pk_partial_ordered_ids_match_exact_knn_without_skip_indexes';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT
    (
        SELECT groupArray(id)
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
        SELECT groupArray(id)
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

-- Check USearchSearchCount for the query above.
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

-- Adaptive granularity: variable row sizes per mark. Vectors are deliberately non-monotonic
-- (id order != distance order) so the ordered assertions below detect a wrong row-to-mark mapping.
DROP TABLE IF EXISTS tab_pk_partial_adaptive;

CREATE TABLE tab_pk_partial_adaptive(
    id Int32,
    pad String,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2
) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 3, index_granularity_bytes = 64, min_index_granularity_bytes = 10, min_bytes_for_wide_part = 0;

-- For reference_vec [0, 0], the L2 distances of rows 6..11 are 3, 5, 1, 4, 2, 6, so the exact
-- top-3 order is [8, 10, 6] which is neither the id order nor a plain sort of the surviving ids.
INSERT INTO tab_pk_partial_adaptive VALUES
    (0, repeat('a', 0), [10.0, 0.0]), (1, repeat('a', 20), [11.0, 0.0]), (2, repeat('a', 40), [12.0, 0.0]),
    (3, repeat('a', 60), [13.0, 0.0]), (4, repeat('a', 80), [14.0, 0.0]), (5, repeat('a', 100), [15.0, 0.0]),
    (6, repeat('a', 120), [0.0, 3.0]), (7, repeat('a', 140), [0.0, 5.0]), (8, repeat('a', 160), [0.0, 1.0]),
    (9, repeat('a', 180), [0.0, 4.0]), (10, repeat('a', 200), [0.0, 2.0]), (11, repeat('a', 220), [0.0, 6.0]);

SELECT id
FROM tab_pk_partial_adaptive
WHERE id >= 6
ORDER BY L2Distance(vec, [toFloat32(0.), toFloat32(0.)]) ASC
LIMIT 3
SETTINGS use_skip_indexes = 1, log_comment = '04217-adaptive-pk-partial'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'pk_partial_adaptive_vector_index_used';
-- Check USearchSearchCount for the adaptive-granularity query above.
SELECT max(ProfileEvents['USearchSearchCount'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04217-adaptive-pk-partial'
    AND event_date >= yesterday()
    AND event_time >= now() - 600;

-- Ordered correctness under adaptive granularity: the filtered_search result must match the
-- brute-force baseline exactly, including order. This guards the row-to-mark mapping that the
-- USearchSearchCount check above cannot (it only proves the index path executed, not that it
-- returned the right rows in the right order).
SELECT 'pk_partial_adaptive_ordered_ids_match_exact_knn_without_skip_indexes';
WITH [toFloat32(0.), toFloat32(0.)] AS reference_vec
SELECT
    (
        SELECT groupArray(id)
        FROM
        (
            SELECT id
            FROM tab_pk_partial_adaptive
            WHERE id >= 6
            ORDER BY L2Distance(vec, reference_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 1
        )
    ) = (
        SELECT groupArray(id)
        FROM
        (
            SELECT id
            FROM tab_pk_partial_adaptive
            WHERE id >= 6
            ORDER BY L2Distance(vec, reference_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 0
        )
    );

SELECT 'pk_partial_adaptive_expected_ordered_top3_ids';
WITH [toFloat32(0.), toFloat32(0.)] AS reference_vec
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM tab_pk_partial_adaptive
    WHERE id >= 6
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);

DROP TABLE tab_pk_partial_adaptive;

-- OR spans two skip-index granules; exact ids may differ from brute force.
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

-- The row counts and the PK membership above do not cover the cross-granule hint merge itself: a regression that drops
-- the hints of one granule, or pairs distances with the wrong rows, can still return three in-filter rows. `id <= 2 OR
-- id >= 6` is aligned with the mark boundaries, so both skip-index granules are searched, no row is removed by the
-- residual predicate, and the index result must match the brute-force baseline exactly - including the order and the
-- distance of every row, which a wrong row-to-distance merge would break.
SELECT 'pk_multi_granule_ordered_ids_and_distances_match_exact_knn_without_skip_indexes';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT
    (
        SELECT groupArray((id, round(L2Distance(vec, reference_vec), 4)))
        FROM
        (
            SELECT id, vec
            FROM tab_pk_partial
            WHERE (id <= 2) OR (id >= 6)
            ORDER BY L2Distance(vec, reference_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 1
        )
    ) = (
        SELECT groupArray((id, round(L2Distance(vec, reference_vec), 4)))
        FROM
        (
            SELECT id, vec
            FROM tab_pk_partial
            WHERE (id <= 2) OR (id >= 6)
            ORDER BY L2Distance(vec, reference_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 0
        )
    );

SELECT 'pk_multi_granule_expected_ordered_ids_and_distances';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT groupArray((id, round(L2Distance(vec, reference_vec), 4)))
FROM
(
    SELECT id, vec
    FROM tab_pk_partial
    WHERE (id <= 2) OR (id >= 6)
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1
);

-- OR with disjoint PK ranges in one skip-index granule.
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

-- Selectivity threshold: if less than 'vector_search_min_surviving_pk_fraction' of the marks of the part survive primary
-- key analysis, the vector index is not used for the part (filtered_search over a small row filter is slower than exact
-- distances for the few surviving rows, and it post-filters, so it can also return fewer rows).
-- `id >= 9` leaves 2 of the 4 marks (the primary key range of the preceding mark ends at the boundary value 9).
SELECT id
FROM tab_pk_partial
WHERE id >= 9
ORDER BY L2Distance(vec, [toFloat32(0.), toFloat32(2.)]) ASC
LIMIT 3
SETTINGS use_skip_indexes = 1, vector_search_min_surviving_pk_fraction = 0.75, log_comment = '04217-pk-below-threshold'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'pk_below_threshold_vector_index_not_used';
SELECT max(ProfileEvents['USearchSearchCount'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04217-pk-below-threshold'
    AND event_date >= yesterday()
    AND event_time >= now() - 600;

-- Below the threshold the result is exact, like before this feature existed.
SELECT 'pk_below_threshold_ordered_ids';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM tab_pk_partial
    WHERE id >= 9
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1, vector_search_min_surviving_pk_fraction = 0.75
);

-- The same query with the threshold disabled goes through the index: the surviving marks of the granule are searched
-- first (`id >= 9` keeps the mark holding ids 6, 7, 8 too, because the primary key ranges end at the mark boundary),
-- and the nearest neighbours found there are then removed by the exact `id >= 9` predicate. Fewer rows than `LIMIT`
-- (here: none) is the expected post-filtering behavior and the reason for the threshold above.
SELECT 'pk_below_threshold_index_path_postfilters';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT groupArray(id)
FROM
(
    SELECT id
    FROM tab_pk_partial
    WHERE id >= 9
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 3
    SETTINGS use_skip_indexes = 1, vector_search_min_surviving_pk_fraction = 0
);

SELECT '-- Out-of-range vector_search_min_surviving_pk_fraction values throw an exception';
WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT id
FROM tab_pk_partial
WHERE id >= 6
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_min_surviving_pk_fraction = -0.1; -- { serverError INVALID_SETTING_VALUE }

WITH [toFloat32(0.), toFloat32(2.)] AS reference_vec
SELECT id
FROM tab_pk_partial
WHERE id >= 6
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_min_surviving_pk_fraction = 1.1; -- { serverError INVALID_SETTING_VALUE }

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
