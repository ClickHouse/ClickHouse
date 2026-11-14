#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: rewrite rules are global server state

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A REJECT rule must not be bypassable by labelling the query as a secondary (distributed)
# query. `ClientInfo::query_kind` comes from the client and can be spoofed
# (`clickhouse-client --query_kind secondary_query`). Rule application no longer trusts it;
# instead the initiator strips `query_rules` from the settings it sends to shards, so a genuine
# secondary query carries no rules, while a spoofing client's own `query_rules` is still enforced.

# The rule name is global, so it is made unique per test database. `DROP RULE` has no
# `IF EXISTS` form, so a leftover from a previous failed run is dropped defensively.
RULE="rule_spoof_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}" 2>/dev/null
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT 1 WHERE 1 = {p:String}) REJECT WITH 'rejected by ${RULE}'"

# Baseline: a normal initial query with the rule active is rejected.
echo "initial query:"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "SELECT 1 WHERE 1 = 'x'" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION"

# The same query sent as a (spoofed) secondary query must still be rejected: a client cannot
# bypass the rule by claiming its query is a distributed fragment.
echo "spoofed secondary query:"
$CLICKHOUSE_CLIENT --query_kind secondary_query --query_rules "${RULE}" --query "SELECT 1 WHERE 1 = 'x'" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
