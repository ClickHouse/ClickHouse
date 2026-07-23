-- Tags: zookeeper

-- Smoke test for the keeper client histogram metrics

-- histograms with the operation_type label
SELECT value > 0
FROM system.histogram_metrics
WHERE name = 'keeper_response_time_ms'
  AND labels['operation_type'] = 'readonly'
  AND labels['le'] = '+Inf';

-- histogram without the operation_type label
SELECT value > 0
FROM system.histogram_metrics
WHERE name = 'keeper_client_queue_duration_milliseconds'
  AND labels['le'] = '+Inf';

-- Numeric bucket boundaries are formatted without trailing zeros (e.g. "100", not "100.000000").
SELECT count()
FROM system.histogram_metrics
WHERE name = 'keeper_client_queue_duration_milliseconds'
  AND labels['le'] = '100';

SYSTEM FLUSH LOGS metric_log;

-- The keeper_client_queue_duration_milliseconds histogram appears in metric_log.
SELECT count() > 0
FROM system.metric_log
ARRAY JOIN histograms AS h
WHERE h.metric = 'keeper_client_queue_duration_milliseconds';

-- The last bucket key is +Inf and its cumulative count equals h.count.
SELECT min(isInfinite(mapKeys(h.histogram)[-1]) AND mapValues(h.histogram)[-1] = h.count)
FROM system.metric_log
ARRAY JOIN histograms AS h
WHERE h.metric = 'keeper_client_queue_duration_milliseconds';

-- Bucket boundaries are ascending.
SELECT min(arraySort(mapKeys(h.histogram)) = mapKeys(h.histogram))
FROM system.metric_log
ARRAY JOIN histograms AS h
WHERE h.metric = 'keeper_client_queue_duration_milliseconds';
