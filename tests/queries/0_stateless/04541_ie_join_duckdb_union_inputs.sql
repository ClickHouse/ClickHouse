-- Tags: no-old-analyzer

-- IEJoin over inputs assembled with UNION ALL (multiple upstream streams).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS calendar7278;
DROP TABLE IF EXISTS snapshot_data;

CREATE TABLE calendar7278 ENGINE = MergeTree ORDER BY tuple() AS
SELECT toDateTime('2023-01-01 06:00:00', 'UTC') + toIntervalHour(12 * number) AS start_ts,
       start_ts + toIntervalHour(12) AS end_ts
FROM numbers(302);

CREATE TABLE snapshot_data ENGINE = MergeTree ORDER BY tuple() AS
SELECT toDateTime('2023-03-01 08:00:00', 'UTC') AS snapshot_ts, 1 AS snapshot_value FROM numbers(1000);

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM snapshot_data data JOIN calendar7278 cal
    ON data.snapshot_ts >= cal.start_ts AND data.snapshot_ts <= cal.end_ts
) WHERE explain LIKE '%IEJoin%';

SELECT count() FROM snapshot_data data JOIN calendar7278 cal
ON data.snapshot_ts >= cal.start_ts AND data.snapshot_ts <= cal.end_ts;

SELECT count() FROM snapshot_data data
JOIN (SELECT * FROM calendar7278 UNION ALL SELECT * FROM calendar7278) cal
ON data.snapshot_ts >= cal.start_ts AND data.snapshot_ts <= cal.end_ts;

SELECT count() FROM snapshot_data data
JOIN (SELECT * FROM calendar7278 UNION ALL SELECT * FROM calendar7278 UNION ALL SELECT * FROM calendar7278) cal
ON data.snapshot_ts >= cal.start_ts AND data.snapshot_ts <= cal.end_ts;

SELECT count() FROM snapshot_data data
JOIN (SELECT * FROM calendar7278 UNION ALL SELECT * FROM calendar7278 UNION ALL SELECT * FROM calendar7278) cal
ON data.snapshot_ts >= cal.start_ts AND data.snapshot_ts <= cal.end_ts
JOIN (SELECT * FROM calendar7278 UNION ALL SELECT * FROM calendar7278 UNION ALL SELECT * FROM calendar7278) cal2
ON data.snapshot_ts >= cal2.start_ts AND data.snapshot_ts <= cal2.end_ts;

DROP TABLE calendar7278;
DROP TABLE snapshot_data;
