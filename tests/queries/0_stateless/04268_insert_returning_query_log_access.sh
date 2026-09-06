#!/usr/bin/env bash
# Issue #21697: INSERT ... RETURNING must record the subquery's table/column access in system.query_log.
# The RETURNING subquery reads a table other than the INSERT target; that read must show up in `tables`.
# Filter the query_log by an explicit query_id so the lookup is deterministic.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_returning_access_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_returning_access_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_returning_access_src (id UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_returning_access_dst (id UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_returning_access_src VALUES (7)"

qid="04268_${CLICKHOUSE_DATABASE}"

# The RETURNING subquery result (7) is emitted as the statement output.
${CLICKHOUSE_CLIENT} --async_insert=0 --query_id="$qid" -q \
    "INSERT INTO t_returning_access_dst (id) RETURNING (SELECT id FROM t_returning_access_src WHERE id = 7) VALUES (1)"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# The subquery's source table must be recorded in the statement's query_log access info.
${CLICKHOUSE_CLIENT} -q "SELECT arrayExists(t -> t LIKE '%t_returning_access_src%', tables) FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$qid' AND type = 'QueryFinish'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_returning_access_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_returning_access_src"
