-- Tags: no-fasttest, no-ordinary-database
-- vector_similarity with partial PK pruning: use_skip_indexes 1 vs 0 must return the same row ids.
-- Also: ORDER BY Date + range on that key + vector ORDER BY LIMIT.

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;

DROP TABLE IF EXISTS tab_pk_partial;

CREATE TABLE tab_pk_partial(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;

INSERT INTO tab_pk_partial VALUES
    (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]),
    (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

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

-- ORDER BY (Date, id), range on Date, vector query (partial PK within one part).
DROP TABLE IF EXISTS tab_time_tickets;

CREATE TABLE tab_time_tickets(
    id Int32,
    created_date Date,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2
) ENGINE = MergeTree ORDER BY (created_date, id) SETTINGS index_granularity = 3;

INSERT INTO tab_time_tickets VALUES
    (1, '2024-01-01', [0.0, 2.0]), (2, '2024-01-02', [0.0, 2.1]), (3, '2024-01-03', [0.0, 2.2]), (4, '2024-01-04', [0.0, 2.3]),
    (5, '2024-01-05', [0.0, 2.4]), (6, '2024-01-06', [0.0, 2.5]), (7, '2024-01-07', [0.0, 2.6]), (8, '2024-01-08', [0.0, 2.7]),
    (9, '2024-01-09', [1.0, 0.0]), (10, '2024-01-10', [1.1, 0.0]), (11, '2024-01-11', [1.2, 0.0]), (12, '2024-01-12', [1.3, 0.0]);

SELECT 'time_filtered_vector_search';
WITH [toFloat32(1.), toFloat32(0.)] AS query_vec
SELECT
    (
        SELECT arraySort(groupArray(id))
        FROM
        (
            SELECT id
            FROM tab_time_tickets
            WHERE created_date >= '2024-01-09'
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
            WHERE created_date >= '2024-01-09'
            ORDER BY L2Distance(vec, query_vec) ASC
            LIMIT 3
            SETTINGS use_skip_indexes = 0
        )
    );

DROP TABLE tab_time_tickets;
