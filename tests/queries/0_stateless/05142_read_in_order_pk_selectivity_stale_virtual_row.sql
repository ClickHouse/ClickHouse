-- When the primary-key selectivity guard rejects read-in-order for an `ORDER BY`, the virtual row
-- conversion that was installed on the read step for that plan must be dropped again. The read can
-- still be switched to an in-order read afterwards - `optimizeDistinctInOrder` and
-- `optimizeAggregationInOrder` deliberately do not apply the guard - and it would then emit virtual
-- rows into the full sort left above it, whose `MergingSortedTransform` is built without virtual row
-- conversions and compares the raw primary-key names of the announced boundary against the query's
-- sort description (a `LOGICAL_ERROR` in debug builds).

DROP TABLE IF EXISTS rio_pk_selectivity_stale_vrow;

CREATE TABLE rio_pk_selectivity_stale_vrow (key UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES rio_pk_selectivity_stale_vrow;

INSERT INTO rio_pk_selectivity_stale_vrow SELECT number FROM numbers(0, 25000);
INSERT INTO rio_pk_selectivity_stale_vrow SELECT number FROM numbers(25000, 25000);
INSERT INTO rio_pk_selectivity_stale_vrow SELECT number FROM numbers(50000, 25000);

-- `key % 8192 != 8192` is always true, so it selects every granule while being opaque to the
-- primary key index: the guard sees a ratio of 1.0 and rejects read-in-order for the `ORDER BY`,
-- while the preliminary `DISTINCT` still asks for an in-order read.
SET max_threads = 4, enable_parallel_replicas = 0, read_in_order_use_virtual_row = 1,
    optimize_read_in_order = 1, optimize_distinct_in_order = 1;

SELECT 'no virtual row when the guard rejected read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT key FROM rio_pk_selectivity_stale_vrow
    WHERE key % 8192 != 8192
    ORDER BY key
    LIMIT 100
    SETTINGS read_in_order_max_primary_key_ratio = 0.1
) WHERE explain LIKE '%VirtualRowTransform%';

-- Control: with the guard disabled the same query keeps read-in-order and does use a virtual row,
-- so the assertion above is about the rejected request and not about virtual rows being off.
SELECT 'virtual row when read-in-order is kept';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT key FROM rio_pk_selectivity_stale_vrow
    WHERE key % 8192 != 8192
    ORDER BY key
    LIMIT 100
    SETTINGS read_in_order_max_primary_key_ratio = 1.
) WHERE explain LIKE '%VirtualRowTransform%';

SELECT 'result';
SELECT count() FROM
(
    SELECT DISTINCT key FROM rio_pk_selectivity_stale_vrow
    WHERE key % 8192 != 8192
    ORDER BY key
    LIMIT 100
    SETTINGS read_in_order_max_primary_key_ratio = 0.1
);

DROP TABLE rio_pk_selectivity_stale_vrow;
