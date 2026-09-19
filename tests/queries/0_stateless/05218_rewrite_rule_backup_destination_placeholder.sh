#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ASTBackupQuery` attaches its destination (`backup_name`) and the base backup / snapshot names
# through `IAST::set`, so they live in `children`: the rewrite-rule matcher binds placeholders in
# them, and the rule-template AST limits see them, without any non-`children` special case. This
# pins that down for the destination, so the invariant is not lost if the class layout changes.
# The rule names are suffixed with the test database because rules are global server state, and
# only whether the rule fired is asserted, so no backup actually runs.

RULE="rule_backup_dest_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}"
}

trap cleanup EXIT
cleanup

# A placeholder inside the destination binds: the rule fires for a matching destination
# and stays silent for a different `Disk` name.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (BACKUP TABLE tbl_${CLICKHOUSE_DATABASE} TO Disk('backups', {p:String})) REJECT WITH 'blocked'"

echo "matching destination:"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "BACKUP TABLE tbl_${CLICKHOUSE_DATABASE} TO Disk('backups', 'b_${CLICKHOUSE_DATABASE}.zip')" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "not rejected"

echo "other disk:"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "BACKUP TABLE tbl_${CLICKHOUSE_DATABASE} TO Disk('other', 'b_${CLICKHOUSE_DATABASE}.zip')" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "not rejected"

# A placeholder of the wrong type for the slot is not bound: the destination argument is a
# string literal, so `{p:Int}` does not match.
echo "int placeholder:"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}"
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (BACKUP TABLE tbl_${CLICKHOUSE_DATABASE} TO Disk('backups', {p:Int})) REJECT WITH 'blocked'"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "BACKUP TABLE tbl_${CLICKHOUSE_DATABASE} TO Disk('backups', 'b_${CLICKHOUSE_DATABASE}.zip')" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "not rejected"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}"

# The rule-template AST limits reach the destination: a deeply nested destination expression
# trips `max_ast_depth`, while the same template fits under a permissive limit.
echo "deep destination, low max_ast_depth:"
$CLICKHOUSE_CLIENT --max_ast_depth 8 --query "CREATE RULE ${RULE} AS (BACKUP TABLE tbl_${CLICKHOUSE_DATABASE} TO Disk('backups', concat(concat(concat(concat(concat(concat(concat('a'))))))))) REJECT WITH 'blocked'" 2>&1 | grep -o -m1 "TOO_DEEP_AST" || echo "not rejected"
echo "deep destination, default limits:"
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (BACKUP TABLE tbl_${CLICKHOUSE_DATABASE} TO Disk('backups', concat(concat(concat(concat(concat(concat(concat('a'))))))))) REJECT WITH 'blocked'" && echo "created"
