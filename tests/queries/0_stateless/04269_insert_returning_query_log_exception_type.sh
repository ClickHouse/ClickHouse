#!/usr/bin/env bash
# Issue #21697: when an INSERT ... RETURNING persists rows but the RETURNING subquery then fails to plan, the
# statement must be logged in system.query_log as a started-and-failed query (ExceptionWhileProcessing), not as
# ExceptionBeforeStart. This is the inlined VALUES path, where the RETURNING pipeline is built only after the
# insert has been pushed and committed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_ret_log_values"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_ret_log_values (id UInt64) ENGINE = Memory"

qid_values="04269_values_${CLICKHOUSE_DATABASE}"

# The RETURNING subquery references a non-existent column, so it fails to plan after the INSERT persists its row.
${CLICKHOUSE_CLIENT} --async_insert=0 --query_id="$qid_values" -q \
    "INSERT INTO t_ret_log_values (id) RETURNING (SELECT no_such_col FROM t_ret_log_values) VALUES (1)" 2>&1 \
    | grep -o -m1 "UNKNOWN_IDENTIFIER" || echo "no error (values)"

# The INSERT must have persisted its row despite the RETURNING failure.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_ret_log_values"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# Excluding the QueryStart row, the remaining entry must be ExceptionWhileProcessing.
${CLICKHOUSE_CLIENT} -q "SELECT type FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$qid_values' AND type != 'QueryStart' ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ret_log_values"
