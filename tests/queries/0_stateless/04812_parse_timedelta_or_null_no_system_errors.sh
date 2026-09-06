#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: reads the global system.errors counter.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# parseTimeDeltaOrNull / parseTimeDeltaOrZero report parse failures through a status return, so
# they construct no Exception and must not move the BAD_ARGUMENTS counter. The bare parseTimeDelta
# on the same rows must move it -- that arm is what makes the zero delta discriminating rather
# than a probe that never executed anything.

$CLICKHOUSE_CLIENT -m -q "
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
"

query_id=$(random_str 12)

# The two failing queries below feed the system.text_log assertions further down. They also
# supply the bare function's BAD_ARGUMENTS increments, so no separate throwing arm is needed.
# The 50-row one runs last so that last_error_format_string below reports its pattern.
$CLICKHOUSE_CLIENT --query_id "${query_id}_2" -q \
    "SELECT parseTimeDelta('1yyr'); -- { serverError BAD_ARGUMENTS }"
$CLICKHOUSE_CLIENT --query_id "${query_id}_1" -q \
    "SELECT sum(parseTimeDelta(s)) FROM (SELECT concat('junk', toString(number)) AS s FROM numbers(50)); -- { serverError BAD_ARGUMENTS }"

$CLICKHOUSE_CLIENT -m -q "
SELECT 'bare function raised: ',
       (SELECT sum(value) FROM system.errors WHERE name = 'BAD_ARGUMENTS' AND NOT remote)
     - (SELECT value FROM parse_timedelta_errors_baseline) > 0;

-- The thrown Exception keeps its own format string rather than collapsing to \"{}\", so failures
-- stay groupable in system.errors and system.text_log.
SELECT last_error_format_string FROM system.errors
    WHERE name = 'BAD_ARGUMENTS' AND NOT remote AND last_error_time > now() - 60;

DROP TABLE parse_timedelta_errors_baseline;
"

# ... and it also keeps each formatted argument, which is what system.text_log.valueN exposes.
# Two different patterns are asserted so the arguments are shown to track the pattern rather
# than being a fixed pair.
$CLICKHOUSE_CLIENT -m -q "
SYSTEM FLUSH LOGS text_log;

SET max_rows_to_read = 0; -- system.text_log can be really big
SET max_threads = 0; -- override random settings, scanning text_log with 1 thread under TSan is too slow

SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND level = 'Error'
    AND message_format_string = 'Invalid argument of function {}, str: \"{}\".'
    AND value1 = 'parseTimeDelta' AND value2 = 'junk0' AND query_id = '${query_id}_1';

SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND level = 'Error'
    AND message_format_string = 'Invalid argument of function {}, can\'t parse the unit: \"{}\".'
    AND value1 = 'parseTimeDelta' AND value2 = 'yyr' AND query_id = '${query_id}_2';
"
