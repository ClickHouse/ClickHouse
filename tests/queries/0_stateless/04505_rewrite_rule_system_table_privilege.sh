#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.query_rules` exposes global rewrite/rejection policies (rule names, table names,
# filters, query text). It must only be readable by users allowed to manage rules, mirroring
# how `system.named_collections` gates its rows behind a grant. A user without the
# CREATE/ALTER/DROP RULE grants must not be able to enumerate rules, even with plain SELECT
# access to the table.

# The rule name is unique per test database, so no pre-existing rule needs cleaning up first
# (and `DROP RULE` has no `IF EXISTS` form). The user is dropped defensively in case a
# previous run in the same database left one behind.
RULE="rule_priv_${CLICKHOUSE_DATABASE}"
USER="user_priv_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER}"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT '${RULE}') REWRITE TO (SELECT 2)"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER} NOT IDENTIFIED"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.query_rules TO ${USER}"

# Without any rule-management grant, the restricted user sees no rows.
echo "rules without grant:"
$CLICKHOUSE_CLIENT --user "${USER}" --query "SELECT count() FROM system.query_rules WHERE name = '${RULE}'"

# After granting a rule-management privilege, the rule becomes visible.
$CLICKHOUSE_CLIENT --query "GRANT CREATE RULE ON *.* TO ${USER}"
echo "rules with grant:"
$CLICKHOUSE_CLIENT --user "${USER}" --query "SELECT count() FROM system.query_rules WHERE name = '${RULE}'"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
$CLICKHOUSE_CLIENT --query "DROP USER ${USER}"
