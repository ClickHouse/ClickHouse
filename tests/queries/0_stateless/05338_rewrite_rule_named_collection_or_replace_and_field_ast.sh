#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CREATE OR REPLACE NAMED COLLECTION` and `CREATE NAMED COLLECTION` must not collide in the tree hash
# that the rewrite-rule matcher uses for semantic equality, so a rule for one form does not over-match
# the other. Only whether the rule fired (rejection) is asserted: `REJECT` fires during traversal,
# before the statement runs.

RULE_CREATE="rule_nc_create_${CLICKHOUSE_DATABASE}"
RULE_DISK="rule_nc_disk_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_CREATE} AS (CREATE NAMED COLLECTION c_secret AS a = 1) REJECT WITH 'blocked'"

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_CREATE}" --query "CREATE NAMED COLLECTION c_secret AS a = 1" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "create exact: rejected"; else echo "create exact: NOT rejected (unexpected)"; fi

out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_CREATE}" --query "CREATE OR REPLACE NAMED COLLECTION c_secret AS a = 1" 2>&1 || true)
if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "create or replace: WRONGLY over-matched"; else echo "create or replace: not over-matched"; fi

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_CREATE}"

# A named-collection value can be an AST kept inside a `Field` (e.g. `disk(...)`), outside `children`.
# A placeholder there can be neither bound nor substituted, so the rule is rejected at DDL time.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_DISK} AS (CREATE NAMED COLLECTION c AS storage = disk(path = {p:String})) REJECT WITH 'blocked'" 2>&1 \
    | grep -o "REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE" | head -1
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_DISK} AS (ALTER NAMED COLLECTION c SET storage = disk(path = {p:String})) REJECT WITH 'blocked'" 2>&1 \
    | grep -o "REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE" | head -1
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE_DISK}"
