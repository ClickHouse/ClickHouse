-- Tags: use-xray

-- https://github.com/ClickHouse/ClickHouse/pull/103854#discussion_r3235932168

SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1 2; -- { serverError BAD_ARGUMENTS }
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 2 1; -- { serverError BAD_ARGUMENTS }
