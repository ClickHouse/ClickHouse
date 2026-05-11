#!/usr/bin/env bash
# Tags: no-replicated-database, no-async-insert

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_definer="user04100_def_${CLICKHOUSE_DATABASE}_$RANDOM"
user_invoker="user04100_inv_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user_definer, $user_invoker;
CREATE USER $user_definer, $user_invoker;

CREATE TABLE $db.t_target (a Int32, b String, c Float64 DEFAULT 0.5) ENGINE = MergeTree ORDER BY a;

CREATE VIEW $db.v_definer
DEFINER = $user_definer SQL SECURITY DEFINER
AS SELECT a, b FROM $db.t_target;

CREATE VIEW $db.v_invoker
DEFINER = $user_definer SQL SECURITY INVOKER
AS SELECT a, b FROM $db.t_target;

CREATE VIEW $db.v_none
SQL SECURITY NONE
AS SELECT a, b FROM $db.t_target;

CREATE VIEW $db.v_definer_where
DEFINER = $user_definer SQL SECURITY DEFINER
AS SELECT a, b FROM $db.t_target WHERE a > 0;
EOF

echo '===== DEFINER without target privileges ====='
# The definer has neither SELECT nor INSERT on the underlying table yet.
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON $db.v_definer TO $user_invoker"
(( $(${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_definer VALUES (1, 'fail')" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK_insert_blocked" || echo "UNEXPECTED"

echo '===== DEFINER with target privileges ====='
# Definer can now write into the target; invoker still has no direct rights on the table.
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON $db.t_target TO $user_definer"
${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_definer VALUES (2, 'via_definer')"
${CLICKHOUSE_CLIENT} --query "SELECT a, b, c FROM $db.t_target ORDER BY a"

# The invoker still cannot touch the underlying table directly.
(( $(${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.t_target VALUES (99, 'direct', 0.0)" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK_direct_blocked" || echo "UNEXPECTED"

echo '===== DEFINER + WHERE constraint ====='
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON $db.v_definer_where TO $user_invoker"
${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_definer_where VALUES (3, 'ok_via_definer')"
(( $(${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_definer_where VALUES (-1, 'rejected')" 2>&1 | grep -c "VIOLATED_CONSTRAINT") >= 1 )) && echo "OK_constraint" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM $db.t_target WHERE a IN (3, -1) ORDER BY a"

echo '===== INVOKER requires invoker privileges ====='
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON $db.v_invoker TO $user_invoker"
# Invoker has rights on the view, but not on the underlying table.
(( $(${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_invoker VALUES (4, 'invoker_no_grant')" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK_invoker_blocked" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON $db.t_target TO $user_invoker"
${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_invoker VALUES (5, 'invoker_ok')"
${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON $db.t_target FROM $user_invoker"

echo '===== SQL SECURITY NONE skips checks ====='
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, INSERT ON $db.v_none TO $user_invoker"
${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_none VALUES (6, 'none_ok')"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM $db.t_target WHERE a IN (5, 6) ORDER BY a"

echo '===== Revoking definer privileges blocks further inserts ====='
${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON $db.t_target FROM $user_definer"
(( $(${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_definer VALUES (7, 'revoked')" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK_revoked" || echo "UNEXPECTED"

echo '===== ALTER ... MODIFY SQL SECURITY ====='
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON $db.t_target TO $user_definer"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $db.v_definer MODIFY SQL SECURITY INVOKER"
# Invoker now needs INSERT on the underlying table.
(( $(${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_definer VALUES (8, 'after_alter')" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK_after_alter_invoker" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $db.v_definer MODIFY SQL SECURITY DEFINER"
${CLICKHOUSE_CLIENT} --user $user_invoker --query "INSERT INTO $db.v_definer VALUES (9, 'restored')"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM $db.t_target WHERE a = 9"

${CLICKHOUSE_CLIENT} <<EOF
DROP VIEW $db.v_definer;
DROP VIEW $db.v_invoker;
DROP VIEW $db.v_none;
DROP VIEW $db.v_definer_where;
DROP TABLE $db.t_target;
DROP USER IF EXISTS $user_definer, $user_invoker;
EOF
