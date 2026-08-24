#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# The following query fails during query interpretation so it throws an ExceptionBeforeStart
EXCEPTION_BEFORE_START_QUERY="WITH
                              (
                                  SELECT sleepEachRow(1)
                              ) AS sub
                              SELECT *
                              FROM
                              (
                                  SELECT *
                                  FROM system.numbers
                                  WHERE number IN (sub)
                              )
                              SETTINGS enable_global_with_statement = 0, enable_analyzer = 1"


# For this query the system.query_log needs to show ExceptionBeforeStart and elapsed seconds <= 1.0
QUERY_ID="${CLICKHOUSE_DATABASE}_$(date +%s)_02883_q1"

# Check that it happens at least once. So that the query has a chance to finish in less than a second despite the sleep in the subquery.
# Occasional longer times are possible due to high system load, the usage of thread fuzzer and sanitizers.
for _ in {1..100}
do
    ${CLICKHOUSE_CLIENT} --query "$EXCEPTION_BEFORE_START_QUERY" --query_id="$QUERY_ID" >/dev/null 2>&1
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    [[ "1" == "$(${CLICKHOUSE_CLIENT} --query "SELECT type == 'ExceptionBeforeStart' AND query_duration_ms <= 1000 FROM system.query_log WHERE current_database = '$CLICKHOUSE_DATABASE' AND query_id='$QUERY_ID' ORDER BY event_time_microseconds DESC LIMIT 1")" ]] && echo 'Ok' && break
    sleep 0.1
done

# Now we test with a query that will take 1+ seconds. The CLI should show that as part of the output format
OK_QUERY_JSON="
WITH (
        SELECT sleepEachRow(1.0)
    ) AS sub
SELECT *, sub
FROM
(
    SELECT *
    FROM system.one
)
FORMAT JSON
SETTINGS enable_global_with_statement = 1"
QUERY_ID_2="${CLICKHOUSE_DATABASE}_$(date +%s)_02883_q2"
${CLICKHOUSE_CLIENT} --query "$OK_QUERY_JSON" --query_id="${QUERY_ID_2}" | grep elapsed | awk '{ if($2 >= 1.0) { print "Greater (Ok)" } else { print "Smaller than expected: " $2 } }'

OK_QUERY_XML="
WITH (
       SELECT sleepEachRow(1.0)
   ) AS sub
SELECT *
FROM
(
   SELECT *, sub
   FROM system.one
)
FORMAT XML
SETTINGS enable_global_with_statement = 1"
QUERY_ID_3="${CLICKHOUSE_DATABASE}_$(date +%s)_02883_q3"
${CLICKHOUSE_CLIENT} --query "$OK_QUERY_XML" --query_id="${QUERY_ID_3}" | grep elapsed | awk  -F '[<>]' '{ if($3 >= 1.0) { print "Greater (Ok)" } else { print "Smaller than expected: " $3 } }'

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
  SELECT
    type,
    query_duration_ms >= 1000 as elapsed_more_than_one_second,
    (toDecimal64(event_time_microseconds, 6) - toDecimal64(query_start_time_microseconds, 6)) > 1.0 AS end_minus_start_more_than_a_second
  FROM system.query_log
  WHERE type='QueryFinish' AND (query_id='$QUERY_ID_2' OR query_id='${QUERY_ID_3}')
  FORMAT Vertical"
