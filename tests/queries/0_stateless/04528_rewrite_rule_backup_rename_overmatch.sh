#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality and,
# for hash-equal nodes, descends only through `children`. That invariant does not hold for every query
# type a rule template accepts. Two more AST classes kept distinguishing state outside both `children`
# and `updateTreeHashImpl`:
#   * `ASTBackupQuery` (its `children` is empty): the object list, the destination and the settings,
#     and its id is only `BackupQuery` / `RestoreQuery`. So `BACKUP TABLE a TO Disk(...)` hashed the
#     same as `BACKUP TABLE b TO Disk(...)`.
#   * `ASTRenameQuery`: the `exchange` / `database` / `dictionary` flags. So `RENAME TABLE a TO b`
#     hashed the same as `EXCHANGE TABLES a AND b`.
# The tree hashes now fold those fields in, so a rule for one statement no longer over-matches the
# other. The rule names are suffixed with the test database so the test stays parallel-safe despite
# the global rule registry, and only whether the rule fired (rejection) is asserted, so the backup /
# rename statements themselves need not actually run.

RULE_BACKUP="rule_backup_overmatch_${CLICKHOUSE_DATABASE}"
RULE_RENAME="rule_rename_overmatch_${CLICKHOUSE_DATABASE}"

# --- BACKUP: two backups that differ only in the table name (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_BACKUP} AS (BACKUP TABLE tbl_secret TO Disk('backups', 'b_${CLICKHOUSE_DATABASE}.zip')) REJECT WITH 'blocked'"

# The exact statement the rule targets is rejected before it runs.
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_BACKUP}" --query "BACKUP TABLE tbl_secret TO Disk('backups', 'b_${CLICKHOUSE_DATABASE}.zip')" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "backup exact: rejected"; else echo "backup exact: NOT rejected (unexpected)"; fi

# A backup of a different table previously over-matched; it must NOT be rejected now. Whatever the
# statement then does (it may fail for an unrelated reason) is irrelevant -- only rejection matters.
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_BACKUP}" --query "BACKUP TABLE tbl_public TO Disk('backups', 'b_${CLICKHOUSE_DATABASE}.zip')" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "backup: different table is WRONGLY over-matched"; else echo "backup: different table is not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_BACKUP}"

# --- RENAME vs EXCHANGE: differ only in the `exchange` flag (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_RENAME} AS (RENAME TABLE t1 TO t2) REJECT WITH 'blocked'"

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_RENAME}" --query "RENAME TABLE t1 TO t2" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "rename exact: rejected"; else echo "rename exact: NOT rejected (unexpected)"; fi

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_RENAME}" --query "EXCHANGE TABLES t1 AND t2" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "rename: EXCHANGE is WRONGLY over-matched"; else echo "rename: EXCHANGE is not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_RENAME}"
