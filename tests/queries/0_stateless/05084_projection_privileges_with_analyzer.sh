#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query of a projection is analyzed and executed over a synthetic table expression. Neither
# writing nor reading a projection may require any privilege beyond the ones for the table itself.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_projection_privileges
(
    a UInt64,
    b String,
    PROJECTION p_agg (SELECT b, sum(a) GROUP BY b),
    PROJECTION p_normal (SELECT a, b ORDER BY b)
)
ENGINE = MergeTree ORDER BY a SETTINGS materialize_projections_on_insert = 1"

${CLICKHOUSE_CLIENT} --query "GRANT INSERT, SELECT, OPTIMIZE ON ${CLICKHOUSE_DATABASE}.t_projection_privileges TO ${user}"

${CLICKHOUSE_CLIENT} --user "${user}" --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_projection_privileges SELECT number % 7, toString(number % 3) FROM numbers(20)"
${CLICKHOUSE_CLIENT} --user "${user}" --query "OPTIMIZE TABLE ${CLICKHOUSE_DATABASE}.t_projection_privileges FINAL"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT b, sum(a) FROM ${CLICKHOUSE_DATABASE}.t_projection_privileges GROUP BY b ORDER BY b SETTINGS force_optimize_projection = 1"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_projection_privileges WHERE b = '1'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_projection_privileges"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
