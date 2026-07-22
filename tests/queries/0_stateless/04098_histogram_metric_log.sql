SYSTEM FLUSH LOGS histogram_metric_log;

SELECT count() > 0 FROM system.histogram_metric_log WHERE event_date >= yesterday() AND event_time >= now() - 600;

SELECT
    countIf(length(histogram) < 2),
    countIf(NOT isInfinite(mapKeys(histogram)[length(histogram)])),
    countIf(mapValues(histogram)[length(histogram)] != count),
    countIf(mapValues(histogram) != arraySort(mapValues(histogram)))
FROM system.histogram_metric_log
WHERE event_date >= yesterday() AND event_time >= now() - 600;

SELECT count() > 0
FROM system.histogram_metric_log
WHERE metric = 'keeper_client_queue_duration_milliseconds'
  AND event_date >= yesterday() AND event_time >= now() - 600;
