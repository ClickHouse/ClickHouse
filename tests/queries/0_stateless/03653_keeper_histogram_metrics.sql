-- Tags: zookeeper

-- Smoke test for the keeper client histogram metrics

-- histograms with the operation_type label
SELECT value > 0
FROM system.histogram_metrics
WHERE name = 'keeper_response_time_ms'
  AND labels['operation_type'] = 'readonly'
  AND labels['le'] = '+Inf';

SELECT value > 0
FROM system.histogram_metrics
WHERE name = 'keeper_client_roundtrip_duration_milliseconds'
  AND labels['operation_type'] = 'readonly'
  AND labels['le'] = '+Inf';

-- histogram without the operation_type label
SELECT value > 0
FROM system.histogram_metrics
WHERE name = 'keeper_client_queue_duration_milliseconds'
  AND labels['le'] = '+Inf';
