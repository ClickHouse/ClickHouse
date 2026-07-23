-- Tags: use-xray, no-fasttest
-- no-fasttest: SYSTEM INSTRUMENT is only compiled with XRay, which the fast-test build omits.

-- A SLEEP argument outside Int64 range is rejected at parse. Otherwise it would be stored as a
-- Float64 that formats back as plain digits (1e20 -> 100000000000000000000), which re-parse as a
-- wide integer and no longer match, aborting the debug-build AST consistency check (found by the
-- query fuzzer with 1e20).
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 1e20; -- { clientError SYNTAX_ERROR }
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 1e19; -- { clientError SYNTAX_ERROR }
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 100000000000000000000; -- { clientError SYNTAX_ERROR }

-- Fractional and integer arguments within range are accepted and survive a format-parse round-trip.
-- formatQuery parses and formats, so nesting it exercises parse -> format -> parse -> format without
-- running the instrumentation.
SELECT formatQuery(formatQuery($$SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 3.2$$));
SELECT formatQuery(formatQuery($$SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 5$$));
SELECT formatQuery(formatQuery($$SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 3.0$$));
