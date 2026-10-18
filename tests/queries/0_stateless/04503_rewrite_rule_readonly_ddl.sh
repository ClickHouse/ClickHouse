#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Rewrite-rule DDL (`CREATE`/`ALTER`/`DROP RULE`) mutates global metadata, so it must be
# rejected under `readonly` and when `allow_ddl = 0`, even though the default user holds
# the `CREATE RULE` / `ALTER RULE` / `DROP RULE` grants. The rule grants are global, so a
# unique name (per test database) avoids colliding with concurrent tests.

RULE="rule_readonly_ddl_${CLICKHOUSE_DATABASE}"

# CREATE RULE is rejected in readonly mode and when DDL is prohibited.
$CLICKHOUSE_CLIENT --readonly 1 --query "CREATE RULE ${RULE} AS (SELECT 1) REWRITE TO (SELECT 2)" 2>&1 | grep -o -m1 "Cannot execute query in readonly mode"
$CLICKHOUSE_CLIENT --allow_ddl 0 --query "CREATE RULE ${RULE} AS (SELECT 1) REWRITE TO (SELECT 2)" 2>&1 | grep -o -m1 "DDL queries are prohibited"

# Create the rule normally so ALTER / DROP rejection can be exercised against it.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT 1) REWRITE TO (SELECT 2)"

# ALTER RULE / DROP RULE are likewise rejected.
$CLICKHOUSE_CLIENT --allow_ddl 0 --query "ALTER RULE ${RULE} AS (SELECT 1) REWRITE TO (SELECT 3)" 2>&1 | grep -o -m1 "DDL queries are prohibited"
$CLICKHOUSE_CLIENT --readonly 1 --query "DROP RULE ${RULE}" 2>&1 | grep -o -m1 "Cannot execute query in readonly mode"

# The rejected statements left the rule untouched (still exactly one rule with this name).
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.query_rules WHERE name = '${RULE}'"

# Clean up the rule created above.
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
