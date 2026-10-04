#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality and,
# for hash-equal nodes, descends only through `children`. That invariant did not hold for the DDL
# `DROP` classes that keep their whole meaning outside `children`: `ASTDropFunctionQuery`,
# `ASTDropNamedCollectionQuery` and `ASTDropResourceQuery` return a constant `getID` and keep the
# dropped object's name (`function_name` / `collection_name` / `resource_name`), the `if_exists`
# flag and the `ON CLUSTER` name as plain members with no `updateTreeHashImpl`. So e.g.
# `DROP FUNCTION f` hashed the same as `DROP FUNCTION g`, and a rule for one over-matched the other.
# The tree hash now folds those fields in. The rule names are suffixed with the test database so the
# test stays parallel-safe despite the global rule registry, and only whether the rule fired
# (rejection) is asserted, so the DROP statements themselves need not actually run (REJECT fires
# during traversal, before name resolution, so the objects need not exist).

RULE_FUNCTION="rule_drop_function_overmatch_${CLICKHOUSE_DATABASE}"
RULE_COLLECTION="rule_drop_collection_overmatch_${CLICKHOUSE_DATABASE}"
RULE_RESOURCE="rule_drop_resource_overmatch_${CLICKHOUSE_DATABASE}"

# --- DROP FUNCTION f vs DROP FUNCTION g: differ only in `function_name` (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_FUNCTION} AS (DROP FUNCTION f_secret) REJECT WITH 'blocked'"

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_FUNCTION}" --query "DROP FUNCTION f_secret" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop function exact: rejected"; else echo "drop function exact: NOT rejected (unexpected)"; fi

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_FUNCTION}" --query "DROP FUNCTION f_other" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop function: other name is WRONGLY over-matched"; else echo "drop function: other name is not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_FUNCTION}"

# --- DROP NAMED COLLECTION a vs DROP NAMED COLLECTION b: differ only in `collection_name`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_COLLECTION} AS (DROP NAMED COLLECTION c_secret) REJECT WITH 'blocked'"

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_COLLECTION}" --query "DROP NAMED COLLECTION c_secret" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop named collection exact: rejected"; else echo "drop named collection exact: NOT rejected (unexpected)"; fi

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_COLLECTION}" --query "DROP NAMED COLLECTION c_other" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop named collection: other name is WRONGLY over-matched"; else echo "drop named collection: other name is not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_COLLECTION}"

# --- DROP RESOURCE r vs DROP RESOURCE s: differ only in `resource_name`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_RESOURCE} AS (DROP RESOURCE r_secret) REJECT WITH 'blocked'"

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_RESOURCE}" --query "DROP RESOURCE r_secret" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop resource exact: rejected"; else echo "drop resource exact: NOT rejected (unexpected)"; fi

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_RESOURCE}" --query "DROP RESOURCE r_other" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "drop resource: other name is WRONGLY over-matched"; else echo "drop resource: other name is not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_RESOURCE}"
