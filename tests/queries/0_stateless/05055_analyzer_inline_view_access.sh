#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# analyzer_inline_views must not bypass the SELECT grant on a view.
# Inlining replaces the view's TableNode with its body before the planner runs, so the planner's
# SELECT check for the view never fires; for a SQL SECURITY DEFINER view the body is also resolved
# under the definer's identity, so the caller is never checked against the underlying tables either.
# Without a grant on the view, the query must be denied whether or not analyzer_inline_views is set.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE USER ${user} NOT IDENTIFIED;

DROP TABLE IF EXISTS secrets;
CREATE TABLE secrets (k String, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO secrets VALUES ('api_key', 'AKIA-9999'), ('stripe', 'sk_live_2222');

DROP VIEW IF EXISTS v_def;
CREATE VIEW v_def SQL SECURITY DEFINER AS SELECT k, v FROM secrets;

DROP VIEW IF EXISTS v_invoker;
CREATE VIEW v_invoker SQL SECURITY INVOKER AS SELECT k, v FROM secrets;
"

echo "-- DEFINER view without any grant, analyzer_inline_views = 1: must be denied --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM v_def SETTINGS analyzer_inline_views = 1" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

echo "-- DEFINER view without any grant, analyzer_inline_views = 0: must be denied --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM v_def SETTINGS analyzer_inline_views = 0" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

echo "-- base table directly: must be denied --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM secrets SETTINGS analyzer_inline_views = 1" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

echo "-- after GRANT SELECT on the DEFINER view: rows returned (definer reads the base table) --"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_def TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM v_def ORDER BY k SETTINGS analyzer_inline_views = 1"

echo "-- INVOKER view: grant on the view but not on the base table, still denied on the base table --"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM v_invoker SETTINGS analyzer_inline_views = 1" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

echo "-- column-restricted grant on the DEFINER view: granted column is readable (non-inlined path) --"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.v_def FROM ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(k) ON ${CLICKHOUSE_DATABASE}.v_def TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT k FROM v_def ORDER BY k SETTINGS analyzer_inline_views = 1"

echo "-- column-restricted grant on the DEFINER view: ungranted column is denied --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT v FROM v_def SETTINGS analyzer_inline_views = 1" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

${CLICKHOUSE_CLIENT} --query "
DROP VIEW IF EXISTS v_def;
DROP VIEW IF EXISTS v_invoker;
DROP TABLE IF EXISTS secrets;
DROP USER IF EXISTS ${user};
"
