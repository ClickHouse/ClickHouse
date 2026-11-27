-- Tags: use-xray

-- SYSTEM INSTRUMENT ADD must reject literal arguments of unsupported types
-- (e.g. NULL) instead of silently consuming them.
-- https://github.com/ClickHouse/ClickHouse/pull/103854

SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY NULL; -- { clientError SYNTAX_ERROR }
