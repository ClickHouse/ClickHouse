-- Tags: use-xray

-- https://github.com/ClickHouse/ClickHouse/issues/105939

EXPLAIN SYNTAX SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'msg';
EXPLAIN SYNTAX SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.1 0.5;
EXPLAIN SYNTAX SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 1 2 3;
