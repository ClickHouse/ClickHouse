#!/usr/bin/env bash
# Tags: no-old-analyzer

# A nested `SETTINGS` change equal to the current value is applied and marks the setting as changed
# in the subquery's context, as at the top level. That flag is what `compatibility` respects
# ("changed manually" settings are left alone), also when `compatibility` sits in a profile or in
# a nested query, so the flag must not be dropped as a "no-op".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Settings profiles are server-global, so the name carries the test database.
PROFILE="p_compat_05175_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS compatibility = '26.7'"

Q="SELECT changed, value FROM system.settings WHERE name = 'enable_group_by_top_k_optimization'"

echo "-- control: the setting is not changed in the session"
${CLICKHOUSE_CLIENT} --query "${Q}"

echo "-- control: compatibility = '26.7' in a nested clause moves the setting to its old default"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (${Q} SETTINGS compatibility = '26.7')"

echo "-- an equal nested assignment is applied and marked changed in the subquery's context, as at the top level"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (${Q} SETTINGS enable_group_by_top_k_optimization = 1)"

echo "-- and stays on the query node"
${CLICKHOUSE_CLIENT} --query "EXPLAIN QUERY TREE SELECT * FROM (${Q} SETTINGS enable_group_by_top_k_optimization = 1)" | grep -c "SETTINGS enable_group_by_top_k_optimization="

echo "-- same clause: an equal assignment before compatibility keeps the current value"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (${Q} SETTINGS enable_group_by_top_k_optimization = 1, compatibility = '26.7')"

echo "-- same clause: an equal assignment before a profile that carries compatibility keeps the current value"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (${Q} SETTINGS enable_group_by_top_k_optimization = 1, profile = '${PROFILE}')"

echo "-- child scope: an equal assignment in the outer subquery protects the setting from compatibility in the inner one"
${CLICKHOUSE_CLIENT} --query "SELECT value FROM (SELECT * FROM (${Q} SETTINGS compatibility = '26.7') SETTINGS enable_group_by_top_k_optimization = 1)"

echo "-- child scope: the same with a profile that carries compatibility in the inner one"
${CLICKHOUSE_CLIENT} --query "SELECT value FROM (SELECT * FROM (${Q} SETTINGS profile = '${PROFILE}') SETTINGS enable_group_by_top_k_optimization = 1)"

echo "-- top level over HTTP: an equal assignment protects the setting from compatibility in a subquery"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT value FROM (${Q} SETTINGS compatibility = '26.7') SETTINGS enable_group_by_top_k_optimization = 1"

${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE ${PROFILE}"
