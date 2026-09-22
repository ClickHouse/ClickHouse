-- An aggregate over a NULL literal resolves to `Nothing`, whose state occupies zero bytes, so an
-- arena hands out one address for every state allocation. The adaptive drain creates the merge
-- destination's states while the local tables' states are still alive, so it must allocate them
-- from an arena no source variant used; otherwise the bucket merge would see a state merged into
-- itself. The cells run the shape that stages and drains delayed records with such states.

SET max_threads = 4;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
-- The adaptive gate requires two-level aggregation to be permitted.
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;

SELECT 'Zero-size aggregate state alone';
SELECT
    (SELECT count() FROM (SELECT toUInt64(number % 60000) AS k, sum(NULL) AS s FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count() FROM (SELECT toUInt64(number % 60000) AS k, sum(NULL) AS s FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1));

SELECT 'Zero-size aggregate state next to a real one';
SELECT
    (SELECT sum(c), count() FROM (SELECT toUInt64(number % 60000) AS k, sum(NULL) AS s, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT sum(c), count() FROM (SELECT toUInt64(number % 60000) AS k, sum(NULL) AS s, count() AS c FROM numbers_mt(300000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1));
