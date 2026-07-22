-- A large numeric SLEEP argument in SYSTEM INSTRUMENT must survive a format-parse round-trip.
-- The value is stored as Float64 and formats back as plain digits (1e20 -> 100000000000000000000),
-- which re-parse as a wide integer; the parser must accept that and coerce it back to the same
-- Float64. Otherwise re-parsing the formatted query throws, which trips the debug-build AST
-- consistency check and aborts the server (found by the query fuzzer).
-- formatQuery parses and formats, so nesting it exercises the parse -> format -> parse -> format
-- round-trip without needing the instrumentation to actually run.

SELECT formatQuery($$SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 1e20$$);
SELECT formatQuery(formatQuery($$SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 1e20$$));

-- A value just above the Int64 range (parsed as UInt64) takes the same Float64 path and round-trips.
SELECT formatQuery(formatQuery($$SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 1e19$$));

-- Small integer and plain float arguments keep their concrete types and round-trip unchanged.
SELECT formatQuery(formatQuery($$SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 5$$));
SELECT formatQuery(formatQuery($$SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 3.2$$));
