-- LOCALTIMESTAMP and LOCALTIME are SQL-standard / PostgreSQL aliases for now().

-- Omitting parentheses for niladic functions is only supported by the analyzer.
SET enable_analyzer = 1;

-- Result type matches now().
SELECT toTypeName(LOCALTIMESTAMP) = toTypeName(now());
SELECT toTypeName(LOCALTIME) = toTypeName(now());

-- The value matches now() within a tolerance. now() reads the clock per call,
-- so two separate now()-family expressions may straddle a one-second boundary.
SELECT abs(toInt64(LOCALTIMESTAMP) - toInt64(now())) <= 1;
SELECT abs(toInt64(LOCALTIME) - toInt64(now())) <= 1;

-- The aliases are case-insensitive, like current_timestamp.
SELECT toTypeName(localtimestamp) = toTypeName(now());
SELECT toTypeName(LocalTimeStamp) = toTypeName(now());
SELECT toTypeName(localtime) = toTypeName(now());
SELECT toTypeName(LocalTime) = toTypeName(now());