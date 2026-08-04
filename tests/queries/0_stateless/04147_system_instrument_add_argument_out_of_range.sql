-- Tags: use-xray

-- A non-negative integer literal larger than `Int64::max` cannot be stored
-- as `Int64` and must be rejected with a parse error rather than silently
-- overflowing into a negative value.
-- https://github.com/ClickHouse/ClickHouse/pull/103854

-- 9223372036854775808 == Int64::max + 1, fits in UInt64 but not Int64.
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 9223372036854775808; -- { clientError SYNTAX_ERROR }
