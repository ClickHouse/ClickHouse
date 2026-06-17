#!/usr/bin/env bash
# Tags: no-parallel
# ^ no-parallel: creates a user (a global object) and relies on table_engines_require_grant.

# ALTER TABLE ... MODIFY ENGINE persists the target engine into the table's CREATE query, so it must
# carry the same TABLE ENGINE access requirement CREATE TABLE enforces. Otherwise a user holding only
# ALTER ORDER BY could install an engine they are not allowed to create (issue #107551, PR #107585).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

trap '${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"' EXIT

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} NOT IDENTIFIED SETTINGS allow_experimental_alter_modify_engine = 1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_engine_access"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_engine_access (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a"
# Structural privilege only: enough for the alter itself, but not for installing a restricted engine.
${CLICKHOUSE_CLIENT} -q "GRANT ALTER ORDER BY ON ${CLICKHOUSE_DATABASE}.t_engine_access TO ${user}"

# Without TABLE ENGINE ON ReplacingMergeTree the alter is denied.
${CLICKHOUSE_CLIENT} --user "${user}" -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_engine_access MODIFY ENGINE = ReplacingMergeTree" 2>&1 \
    | grep -om1 "necessary to have the grant TABLE ENGINE ON ReplacingMergeTree"

# Granting it lets the same alter through.
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON ReplacingMergeTree TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.t_engine_access MODIFY ENGINE = ReplacingMergeTree"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_engine_access"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_engine_access"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_engine_access'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_engine_access"
