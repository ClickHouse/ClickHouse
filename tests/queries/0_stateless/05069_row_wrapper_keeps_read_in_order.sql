-- The rewrite runs before `optimizeReadInOrder`, which matches the sorting key against the columns
-- the reading step produces. Serving a sorting key column from the wrapper would silently turn a
-- merge of already sorted streams into a full sort.

DROP TABLE IF EXISTS row_wrapper_read_in_order;

SET allow_experimental_row_type = 1;
CREATE TABLE row_wrapper_read_in_order
(
    a UInt64,
    b UInt64,
    c UInt64,
    w Row(a UInt64, b UInt64, c UInt64) MATERIALIZED tuple(a, b, c)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO row_wrapper_read_in_order (a, b, c) SELECT number, number * 2, number * 3 FROM numbers(1000);
INSERT INTO row_wrapper_read_in_order (a, b, c) SELECT number + 500, number, number FROM numbers(1000);

SELECT countIf(explain LIKE '%InOrder%') > 0 FROM (
    EXPLAIN PIPELINE SELECT a, b, c FROM row_wrapper_read_in_order ORDER BY a
        SETTINGS query_plan_use_row_wrappers = 1, optimize_read_in_order = 1,
                 read_in_order_two_level_merge_threshold = 0, max_threads = 4, enable_parallel_replicas = 0
);

-- The two columns outside the sorting key are still served from the wrapper, while the key column
-- stays a direct read.
SELECT countIf(trimLeft(explain) = 'Output: a, w') FROM (
    EXPLAIN header = 1 SELECT a, b, c FROM row_wrapper_read_in_order
        SETTINGS query_plan_use_row_wrappers = 1, enable_parallel_replicas = 0
);

SELECT sum(a), sum(b), sum(c) FROM row_wrapper_read_in_order SETTINGS query_plan_use_row_wrappers = 1;
SELECT sum(a), sum(b), sum(c) FROM row_wrapper_read_in_order SETTINGS query_plan_use_row_wrappers = 0;

DROP TABLE row_wrapper_read_in_order;
