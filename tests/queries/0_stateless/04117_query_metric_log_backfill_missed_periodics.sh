#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel
#
# Regression test for `system.query_metric_log` losing rows when the
# `BackgroundSchedulePool` falls behind: a scheduled `collectMetric` task
# fires after the query has finished, returns early because the query is
# no longer in the `ProcessList`, and the periodic row is silently dropped.
# A timestamp collision between the last periodic and the finish moment
# can also drop the final "query finished" row through the dedup guard.
#
# The bug is timing-deep and only surfaces naturally on TSan CI builds at
# ~0.1% rate. We make it deterministic via the `query_metric_log_delay_collect`
# failpoint, which sleeps inside `collectMetric` so the captured `current_time`
# slips past the query's finish moment.
#
# `no-parallel` tag: the failpoint is server-wide; parallel test instances
# would race on enable/disable and produce false negatives.
#
# Without the fix: 2 rows for the query (the canonical CI failure pattern).
# With the fix:    >= 3 rows (one or more periodics + backfilled missed +
#                  forced final via `is_final=true`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

readonly query_id="${CLICKHOUSE_DATABASE}_$$"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT query_metric_log_delay_collect"
trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT query_metric_log_delay_collect"' EXIT

${CLICKHOUSE_CLIENT} --query-id="${query_id}" -q "
SELECT sleep(2.5)
SETTINGS query_metric_log_interval=1000,
         function_sleep_max_microseconds_per_block=10000000000,
         enable_parallel_replicas=0
FORMAT Null"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_metric_log"

${CLICKHOUSE_CLIENT} -q "
SELECT count() >= 3
FROM system.query_metric_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND query_id = '${query_id}'"
