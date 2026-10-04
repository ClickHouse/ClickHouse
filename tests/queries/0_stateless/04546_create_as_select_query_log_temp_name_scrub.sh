#!/usr/bin/env bash
# Regression test for the plain `CREATE TABLE ... AS SELECT` temporary-table publish path (issue #26746):
# the internal `_tmp_replace_*` table name must not leak into `system.query_log` `tables`, not only on the
# successful publish (which was already scrubbed) but also when the query fails after the populating
# INSERT SELECT has touched the temporary table. `executeQuery.cpp` copies the query's access info into
# `system.query_log` for failed queries too, so the failure/rethrow path must scrub the temporary name just
# like the success path does; otherwise a denied or failing `CREATE ... AS SELECT` leaks a meaningless,
# non-deterministic internal name into the log.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

fail_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_fail"

# The populating INSERT SELECT records the temporary target in the query's access info, then `throwIf(1)`
# fails the query during execution -- exercising the generic exception path of the temporary-table publish.
echo "-- the failing CREATE ... AS SELECT must report the SELECT's error:"
${CLICKHOUSE_CLIENT} --query_id "${fail_id}" --query \
    "CREATE TABLE dst_fail ENGINE = MergeTree ORDER BY x AS SELECT throwIf(1) AS x" 2>&1 \
    | grep -Fo "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" | uniq

echo "-- the failed query must not leave an orphan table:"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE dst_fail"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "-- the internal temporary table name must not appear in system.query_log 'tables' for the failed query:"
${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday()
  AND current_database = currentDatabase()
  AND query_id = '${fail_id}'
  AND arrayExists(t -> t LIKE '%\\_tmp\\_replace\\_%', tables)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS dst_fail"
