#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality and,
# for hash-equal nodes, descends only through `children`. That invariant did not hold for `ASTDropQuery`:
# its id (`getID`) only distinguishes the `kind` (DROP / DETACH / TRUNCATE) and the database / table
# names, while the flags that decide what is actually dropped and how -- `is_view`, `is_dictionary`,
# `if_exists`, `if_empty`, `has_all`, `has_tables`, the `like` pattern with its `not_like` /
# `case_insensitive_like` modifiers, `sync`, `permanently`, `TEMPORARY` and the `ON CLUSTER` name --
# are plain members kept outside both `children` and `updateTreeHashImpl`. So e.g. `DROP VIEW v`
# hashed the same as `DROP TABLE v`, and a rule for one over-matched the other. The tree hash now
# folds those fields in. The rule names are suffixed with the test database so the test stays
# parallel-safe despite the global rule registry, and only whether the rule fired (rejection) is
# asserted, so the DROP statements themselves need not actually run (REJECT fires during traversal,
# before name resolution, so the objects need not exist).

RULE_VIEW="rule_drop_view_overmatch_${CLICKHOUSE_DATABASE}"
RULE_SYNC="rule_drop_sync_overmatch_${CLICKHOUSE_DATABASE}"
RULE_IF_EXISTS="rule_drop_if_exists_overmatch_${CLICKHOUSE_DATABASE}"

# --- DROP VIEW vs DROP TABLE: differ only in the `is_view` flag (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_VIEW} AS (DROP VIEW v_secret) REJECT WITH 'blocked'"

# The exact statement the rule targets is rejected before it runs.
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_VIEW}" --query "DROP VIEW v_secret" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop view exact: rejected"; else echo "drop view exact: NOT rejected (unexpected)"; fi

# Dropping the same name as a plain table previously over-matched; it must NOT be rejected now.
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_VIEW}" --query "DROP TABLE v_secret" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop view: DROP TABLE is WRONGLY over-matched"; else echo "drop view: DROP TABLE is not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_VIEW}"

# --- DROP TABLE ... SYNC vs DROP TABLE: differ only in the `sync` flag (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_SYNC} AS (DROP TABLE t_secret SYNC) REJECT WITH 'blocked'"

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_SYNC}" --query "DROP TABLE t_secret SYNC" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop sync exact: rejected"; else echo "drop sync exact: NOT rejected (unexpected)"; fi

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_SYNC}" --query "DROP TABLE t_secret" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop sync: non-SYNC DROP is WRONGLY over-matched"; else echo "drop sync: non-SYNC DROP is not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_SYNC}"

# --- DROP TABLE IF EXISTS vs DROP TABLE: differ only in the `if_exists` flag (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_IF_EXISTS} AS (DROP TABLE IF EXISTS t_secret) REJECT WITH 'blocked'"

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_IF_EXISTS}" --query "DROP TABLE IF EXISTS t_secret" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop if exists exact: rejected"; else echo "drop if exists exact: NOT rejected (unexpected)"; fi

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_IF_EXISTS}" --query "DROP TABLE t_secret" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop if exists: non-IF-EXISTS DROP is WRONGLY over-matched"; else echo "drop if exists: non-IF-EXISTS DROP is not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_IF_EXISTS}"
