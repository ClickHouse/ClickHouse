#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RULE="rr_drop_if_exists_${CLICKHOUSE_DATABASE}"
GUARD="rr_drop_if_exists_guard_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}"
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${GUARD}"
}

trap cleanup EXIT
cleanup

echo "missing, IF EXISTS:"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}" && echo "ok"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT 'nomatch') REWRITE TO (SELECT 1)"

echo "existing, IF EXISTS:"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}" && echo "ok"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.query_rules WHERE name = '${RULE}'"

echo "missing, without IF EXISTS:"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}" 2>&1 | grep -o -m1 "REWRITE_RULE_DOESNT_EXIST"

echo "formatting:"
$CLICKHOUSE_CLIENT --query "SELECT formatQuery('DROP RULE IF EXISTS some_rule')"

# `IF EXISTS` is not part of `children`, so it has to be folded into the tree hash:
# a template with it must not match a `DROP RULE` without it, and the other way round.
echo "matching:"
$CLICKHOUSE_CLIENT --query "CREATE RULE ${GUARD} AS (DROP RULE IF EXISTS ${RULE}) REJECT WITH 'blocked'"
$CLICKHOUSE_CLIENT --query_rules "${GUARD}" --query "DROP RULE IF EXISTS ${RULE}" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION"
$CLICKHOUSE_CLIENT --query_rules "${GUARD}" --query "DROP RULE ${RULE}" 2>&1 | grep -o -m1 "REWRITE_RULE_DOESNT_EXIST"
