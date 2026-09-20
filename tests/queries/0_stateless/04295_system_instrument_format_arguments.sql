-- Tags: use-xray

-- https://github.com/ClickHouse/ClickHouse/issues/105939

SELECT formatQuerySingleLine('SYSTEM INSTRUMENT ADD ''QueryMetricLog::startQuery'' LOG ENTRY ''msg''');
SELECT formatQuerySingleLine('SYSTEM INSTRUMENT ADD ''QueryMetricLog::startQuery'' SLEEP ENTRY 0.1 0.5');
SELECT formatQuerySingleLine('SYSTEM INSTRUMENT ADD ''QueryMetricLog::startQuery'' SLEEP ENTRY 1 2 3');
