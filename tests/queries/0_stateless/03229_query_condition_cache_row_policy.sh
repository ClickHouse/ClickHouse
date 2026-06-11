#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica, so the poisoning is
#                       deterministic only on a single replica

# Regression test for the query condition cache being poisoned by a row-level security filter.
# A row policy is prepended as a filter ahead of PREWHERE (getPrewhereActions), so a granule can
# become fully non-matching only because the policy hides its rows. The PREWHERE attribution write
# keys only on the query PREWHERE predicate's hash (not the row policy), so a user whose policy hides
# a mark used to populate a cache entry for that mark; a later query by a user that should see the
# mark then read the prewhere-only key and wrongly skipped rows. index_granularity = 1 makes the
# effect deterministic because each granule holds a single row.
#
# Two users, both with a row policy (so neither hits throw_on_unmatched_row_policies, which is on in
# the test config): u_hide is restricted to v < 50, u_show has a permissive (always-true) policy and
# must see all rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# User and policy names are server-global, suffix them with the (unique) test database to avoid clashes.
user_hide="u_hide_${CLICKHOUSE_DATABASE}"
user_show="u_show_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_hide}, ${user_show}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_qcc_row_policy;
CREATE TABLE t_qcc_row_policy (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_qcc_row_policy SELECT number, number FROM numbers(100);

CREATE USER ${user_hide} NOT IDENTIFIED;
CREATE USER ${user_show} NOT IDENTIFIED;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy TO ${user_hide}, ${user_show};

-- ${user_hide} sees only v < 50; ${user_show} has an always-true policy and sees every row.
CREATE ROW POLICY rp_hide_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy FOR SELECT USING v < 50 TO ${user_hide};
CREATE ROW POLICY rp_show_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy FOR SELECT USING 1 TO ${user_show};

SYSTEM DROP QUERY CONDITION CACHE;
"

# The hide user runs PREWHERE v >= 50 first. Its policy (v < 50) hides every matching row, so the
# result is 0 -- and without the fix this writes a poisoned 'non-matching' cache entry for those marks.
echo "-- hide user (sees only v < 50), PREWHERE v >= 50 -> 0"
${CLICKHOUSE_CLIENT} --user "${user_hide}" --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_row_policy PREWHERE v >= 50 SETTINGS use_query_condition_cache = 1"

# The show user runs the same PREWHERE. Its always-true policy lets it see all 50 rows (v in 50..99).
# Reading the prewhere-only cache entry written above would wrongly return fewer.
echo "-- show user (always-true policy), same PREWHERE v >= 50 -> 50"
${CLICKHOUSE_CLIENT} --user "${user_show}" --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_row_policy PREWHERE v >= 50 SETTINGS use_query_condition_cache = 1"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP ROW POLICY rp_hide_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy;
DROP ROW POLICY rp_show_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy;
DROP TABLE t_qcc_row_policy;
DROP USER ${user_hide}, ${user_show};
"
