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

# A view can expose ALIAS columns, which the planner checks as separate SELECT privileges. The
# inline gate must cover them (getAll, not getOrdinary), otherwise an ALIAS column the caller has
# no grant on would still be inlined and its SELECT check skipped.
${CLICKHOUSE_CLIENT} --query "
DROP VIEW IF EXISTS v_alias;
CREATE VIEW v_alias (k String, secret String ALIAS upper(k)) SQL SECURITY DEFINER AS SELECT k FROM secrets;
GRANT SELECT(k) ON ${CLICKHOUSE_DATABASE}.v_alias TO ${user};
"

echo "-- ALIAS column without a grant on it: must be denied even with inlining --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT secret FROM v_alias SETTINGS analyzer_inline_views = 1" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

echo "-- granted ordinary column of the same view is still readable --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT k FROM v_alias ORDER BY k SETTINGS analyzer_inline_views = 1"

# SQL SECURITY NONE is the most permissive mode: its body is resolved under a no-user global
# context, so the inner tables are read unchecked. The caller must still hold SELECT on the view
# itself, and the inline gate (which runs before the security-type branch) must enforce that.
${CLICKHOUSE_CLIENT} --query "
DROP VIEW IF EXISTS v_none;
CREATE VIEW v_none SQL SECURITY NONE AS SELECT k, v FROM secrets;
"

echo "-- SQL SECURITY NONE view without a grant: must be denied even with inlining --"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM v_none SETTINGS analyzer_inline_views = 1" 2>&1 | grep -o "ACCESS_DENIED" | head -n 1

echo "-- SQL SECURITY NONE view after GRANT SELECT: rows returned (inner table read with no user) --"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_none TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT * FROM v_none ORDER BY k SETTINGS analyzer_inline_views = 1"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW IF EXISTS v_none;
DROP VIEW IF EXISTS v_alias;
DROP VIEW IF EXISTS v_def;
DROP VIEW IF EXISTS v_invoker;
DROP TABLE IF EXISTS secrets;
DROP USER IF EXISTS ${user};
"
