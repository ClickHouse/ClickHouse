-- Tags: no-parallel-replicas
-- `HAVING` prefiltering requires final aggregation; parallel replicas split that stage.
-- Retained `Direct` keys must preserve source metadata while rejected groups destroy
-- their non-trivial `groupArray` states. The prefilter setting must not affect the result.
SET allow_experimental_time_decay_aggregate_functions = 1;
SET serialize_query_plan = 0;
SET query_plan_enable_optimizations = 1;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_bytes_before_external_group_by = 10737418240;
SET adaptive_aggregator_freeze_threshold = 0;
SET optimize_aggregation_in_order = 0;
SET query_plan_merge_filters = 1;
SET max_rows_to_group_by = 0;
SET max_block_size = 2;

DROP TABLE IF EXISTS time_decay_having_64;
CREATE TABLE time_decay_having_64
(
    curve ExponentialTimeDecaying64(10, direct),
    payload String
)
ENGINE = Memory;
INSERT INTO time_decay_having_64
SELECT CAST((value, time), 'ExponentialTimeDecaying64(10, direct)'), payload
FROM VALUES('value Float64, time Float64, payload String',
    (2, 0, 'a'), (2, 0, 'b'), (2, 0, 'c'),
    (4, 1, 'd'), (4, 1, 'e'), (8, 2, 'f'));
SET query_plan_aggregation_having_prefilter = 1;
SELECT '64 prefilter', count() FROM (EXPLAIN actions = 1
    SELECT curve, groupArray(payload), count()
    FROM time_decay_having_64 GROUP BY curve HAVING count() > 1
) WHERE explain LIKE '%HAVING pre-filter: count() > 1%';

SET query_plan_aggregation_having_prefilter = 0;
SELECT 64, 0, tupleElement(curve, 'value'), tupleElement(curve, 'timestamp'),
       arraySort(groupArray(payload)), count()
FROM time_decay_having_64
GROUP BY curve HAVING count() > 1
ORDER BY tupleElement(curve, 'value');

SET query_plan_aggregation_having_prefilter = 1;
SELECT 64, 1, tupleElement(curve, 'value'), tupleElement(curve, 'timestamp'),
       arraySort(groupArray(payload)), count()
FROM time_decay_having_64
GROUP BY curve HAVING count() > 1
ORDER BY tupleElement(curve, 'value');

SELECT '64 all rejected', count(), sum(length(payloads)) FROM
(
    SELECT curve, groupArray(payload) AS payloads
    FROM time_decay_having_64 GROUP BY curve HAVING count() > 100
);
DROP TABLE time_decay_having_64;

DROP TABLE IF EXISTS time_decay_having_128;
CREATE TABLE time_decay_having_128
(
    curve ExponentialTimeDecaying128(10, direct),
    payload String
)
ENGINE = Memory;
INSERT INTO time_decay_having_128
SELECT CAST((value, time), 'ExponentialTimeDecaying128(10, direct)'), payload
FROM VALUES('value Float64, time Float64, payload String',
    (2, 0, 'a'), (2, 0, 'b'), (2, 0, 'c'),
    (4, 1, 'd'), (4, 1, 'e'), (8, 2, 'f'));
SET query_plan_aggregation_having_prefilter = 1;
SELECT '128 prefilter', count() FROM (EXPLAIN actions = 1
    SELECT curve, groupArray(payload), count()
    FROM time_decay_having_128 GROUP BY curve HAVING count() > 1
) WHERE explain LIKE '%HAVING pre-filter: count() > 1%';

SET query_plan_aggregation_having_prefilter = 0;
SELECT 128, 0, tupleElement(curve, 'value'), tupleElement(curve, 'timestamp'),
       arraySort(groupArray(payload)), count()
FROM time_decay_having_128
GROUP BY curve HAVING count() > 1
ORDER BY tupleElement(curve, 'value');

SET query_plan_aggregation_having_prefilter = 1;
SELECT 128, 1, tupleElement(curve, 'value'), tupleElement(curve, 'timestamp'),
       arraySort(groupArray(payload)), count()
FROM time_decay_having_128
GROUP BY curve HAVING count() > 1
ORDER BY tupleElement(curve, 'value');

SELECT '128 all rejected', count(), sum(length(payloads)) FROM
(
    SELECT curve, groupArray(payload) AS payloads
    FROM time_decay_having_128 GROUP BY curve HAVING count() > 100
);
DROP TABLE time_decay_having_128;
