-- Tags: no-parallel
-- Tag no-parallel: reads the global system.errors counter.

-- parseTimeDeltaOrNull / parseTimeDeltaOrZero report parse failures through a status return, so
-- they construct no Exception and must not move the BAD_ARGUMENTS counter. The bare parseTimeDelta
-- on the same rows must move it -- that arm is what makes the zero delta discriminating rather
-- than a probe that never executed anything.

DROP TABLE IF EXISTS parse_timedelta_errors_baseline;
CREATE TABLE parse_timedelta_errors_baseline (value UInt64) ENGINE = Memory;

INSERT INTO parse_timedelta_errors_baseline
    SELECT sum(value) FROM system.errors WHERE name = 'BAD_ARGUMENTS' AND NOT remote;

-- sum() forces the projection to be evaluated; count() would prune it away.
SELECT sum(assumeNotNull(parseTimeDeltaOrNull(s))), sum(parseTimeDeltaOrZero(s))
    FROM (SELECT concat('junk', toString(number)) AS s FROM numbers(50));

SELECT 'recovering variants raised nothing: ',
       (SELECT sum(value) FROM system.errors WHERE name = 'BAD_ARGUMENTS' AND NOT remote)
     - (SELECT value FROM parse_timedelta_errors_baseline) = 0;

SELECT sum(parseTimeDelta(s))
    FROM (SELECT concat('junk', toString(number)) AS s FROM numbers(50)); -- { serverError BAD_ARGUMENTS }

SELECT 'bare function raised: ',
       (SELECT sum(value) FROM system.errors WHERE name = 'BAD_ARGUMENTS' AND NOT remote)
     - (SELECT value FROM parse_timedelta_errors_baseline) > 0;

-- The thrown Exception keeps its own format string rather than collapsing to "{}", so failures stay
-- groupable in system.errors / system.text_log / query_log.
SELECT last_error_format_string FROM system.errors
    WHERE name = 'BAD_ARGUMENTS' AND NOT remote AND last_error_time > now() - 60;

DROP TABLE parse_timedelta_errors_baseline;
