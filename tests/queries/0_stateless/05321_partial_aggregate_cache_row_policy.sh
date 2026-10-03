#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# no-parallel: Messes with internal cache.
# no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

# Partial aggregate cache: the cache is shared by all users, and row policies are not part of its key.
# A user restricted by a row policy must not get the per-part states cached by an unrestricted user.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user_full="user_full_${CLICKHOUSE_DATABASE}"
user_restricted="user_restricted_${CLICKHOUSE_DATABASE}"
policy="policy_${CLICKHOUSE_DATABASE}"

settings="SETTINGS use_partial_aggregate_cache = 1, optimize_aggregation_in_order = 0, max_rows_to_group_by = 0, group_by_overflow_mode = 'throw'"

$CLICKHOUSE_CLIENT -m -q "
SYSTEM DROP AGGREGATE CACHE;
DROP TABLE IF EXISTS test_partial_agg_cache_row_policy;
DROP USER IF EXISTS ${user_full}, ${user_restricted};
DROP ROW POLICY IF EXISTS ${policy} ON test_partial_agg_cache_row_policy;

CREATE TABLE test_partial_agg_cache_row_policy (k UInt32, v Int64) ENGINE = MergeTree() ORDER BY k;
SYSTEM STOP MERGES test_partial_agg_cache_row_policy;
INSERT INTO test_partial_agg_cache_row_policy VALUES (1, 10), (1, 100), (2, 20), (2, 200);

CREATE USER ${user_full} IDENTIFIED WITH no_password;
CREATE USER ${user_restricted} IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.test_partial_agg_cache_row_policy TO ${user_full}, ${user_restricted};
CREATE ROW POLICY ${policy} ON test_partial_agg_cache_row_policy USING v < 100 AS RESTRICTIVE TO ${user_restricted};
"

query="SELECT k, sum(v) FROM ${CLICKHOUSE_DATABASE}.test_partial_agg_cache_row_policy GROUP BY k ORDER BY k ${settings}"

echo "--- Unrestricted user, twice"
$CLICKHOUSE_CLIENT --user "${user_full}" -q "${query}"
$CLICKHOUSE_CLIENT --user "${user_full}" -q "${query}"
echo "--- Restricted user sees only the rows allowed by the row policy"
$CLICKHOUSE_CLIENT --user "${user_restricted}" -q "${query}"
echo "--- Unrestricted user again"
$CLICKHOUSE_CLIENT --user "${user_full}" -q "${query}"

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY ${policy} ON test_partial_agg_cache_row_policy;
DROP USER ${user_full}, ${user_restricted};
DROP TABLE test_partial_agg_cache_row_policy;
"
