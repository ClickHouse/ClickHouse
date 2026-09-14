SET max_threads = 1;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS prewhere_uniform_filter;

CREATE TABLE prewhere_uniform_filter
(
    id UInt64,
    all_pass UInt8,
    all_drop UInt8,
    mixed UInt8,
    tail UInt8,
    almost_all UInt8,
    nullable_filter Nullable(UInt8),
    payload UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO prewhere_uniform_filter
SELECT
    number,
    2,
    0,
    toUInt8(number % 2),
    toUInt8(number % 4 < 2),
    toUInt8(number != 9),
    if(number % 3 = 0, NULL, toUInt8(1)),
    number + 1
FROM numbers(10);

SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE all_pass;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE all_drop;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE mixed;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE tail;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE almost_all;
SELECT count(), sum(payload) FROM prewhere_uniform_filter PREWHERE nullable_filter;

DROP TABLE prewhere_uniform_filter;

-- Keep the columns sparse in the part, then restrict each query to the first
-- granule so the sparse filter is uniform true, uniform false, or mixed.
DROP TABLE IF EXISTS prewhere_uniform_filter_sparse;

CREATE TABLE prewhere_uniform_filter_sparse
(
    id UInt64,
    sparse_true UInt8,
    sparse_false UInt8,
    sparse_mixed UInt8,
    nullable_sparse_true Nullable(UInt8),
    nullable_sparse_false Nullable(UInt8),
    nullable_sparse_mixed Nullable(UInt8),
    payload UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 4,
    ratio_of_defaults_for_sparse_serialization = 0.1,
    compute_exact_num_defaults_for_sparse_columns = 1,
    nullable_serialization_version = 'allow_sparse';

INSERT INTO prewhere_uniform_filter_sparse
SELECT
    number,
    if(number < 4, toUInt8(2), toUInt8(0)),
    if(number >= 4 AND number < 8, toUInt8(2), toUInt8(0)),
    if(number < 4 AND number % 2 = 0, toUInt8(2), toUInt8(0)),
    if(number < 4, toUInt8(2), NULL),
    if(number >= 4 AND number < 8, toUInt8(2), NULL),
    if(number < 4 AND number % 2 = 0, toUInt8(2), NULL),
    number + 1
FROM numbers(10);

SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 'prewhere_uniform_filter_sparse'
  AND active
  AND column IN ('sparse_true', 'sparse_false', 'sparse_mixed',
                 'nullable_sparse_true', 'nullable_sparse_false', 'nullable_sparse_mixed')
ORDER BY column;

SELECT count(), sum(payload) FROM prewhere_uniform_filter_sparse PREWHERE sparse_true WHERE id < 4;
SELECT count(), sum(payload) FROM prewhere_uniform_filter_sparse PREWHERE sparse_false WHERE id < 4;
SELECT count(), sum(payload) FROM prewhere_uniform_filter_sparse PREWHERE sparse_mixed WHERE id < 4;
SELECT count(), sum(payload) FROM prewhere_uniform_filter_sparse PREWHERE nullable_sparse_true WHERE id < 4;
SELECT count(), sum(payload) FROM prewhere_uniform_filter_sparse PREWHERE nullable_sparse_false WHERE id < 4;
SELECT count(), sum(payload) FROM prewhere_uniform_filter_sparse PREWHERE nullable_sparse_mixed WHERE id < 4;

DROP TABLE prewhere_uniform_filter_sparse;
