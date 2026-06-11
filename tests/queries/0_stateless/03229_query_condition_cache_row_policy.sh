#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica, so the poisoning is
#                       deterministic only on a single replica

# Regression test for the query condition cache being poisoned by a row-level security filter.
# A row policy is prepended as a filter ahead of PREWHERE (getPrewhereActions), so a granule can
# become fully non-matching only because the policy hides its rows. The PREWHERE attribution write
# keys only on the query PREWHERE predicate's hash (not the row policy), so a user whose policy hides
# a mark used to populate a cache entry for that mark; a later query by a user without that policy
# then read the prewhere-only key and wrongly skipped rows it should see. index_granularity = 1
# makes the effect deterministic because each granule holds a single row.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# User names are server-global, suffix them with the (unique) test database to avoid clashes.
user_pol="u_pol_${CLICKHOUSE_DATABASE}"
user_nopol="u_nopol_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_pol}, ${user_nopol}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_qcc_row_policy;
CREATE TABLE t_qcc_row_policy (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_qcc_row_policy SELECT number, number FROM numbers(100);

CREATE USER ${user_pol} NOT IDENTIFIED;
CREATE USER ${user_nopol} NOT IDENTIFIED;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy TO ${user_pol}, ${user_nopol};

-- Permissive policy: it restricts only ${user_pol}; ${user_nopol} is not in TO and sees all rows.
CREATE ROW POLICY rp_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy FOR SELECT USING v < 50 TO ${user_pol};

SYSTEM DROP QUERY CONDITION CACHE;
"

# The policy user runs PREWHERE v >= 50 first. Its own policy (v < 50) hides every matching row, so the
# result is 0 -- and without the fix this writes a poisoned 'non-matching' cache entry for those marks.
echo "-- policy user (sees only v < 50), PREWHERE v >= 50 -> 0"
${CLICKHOUSE_CLIENT} --user "${user_pol}" --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_row_policy PREWHERE v >= 50 SETTINGS use_query_condition_cache = 1"

# The no-policy user runs the same PREWHERE. It has no row-level filter, so it must see all 50 rows
# (v in 50..99). Reading the prewhere-only cache entry written above would wrongly return fewer.
echo "-- no-policy user, same PREWHERE v >= 50 -> 50"
${CLICKHOUSE_CLIENT} --user "${user_nopol}" --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_row_policy PREWHERE v >= 50 SETTINGS use_query_condition_cache = 1"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP ROW POLICY rp_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy;
DROP TABLE t_qcc_row_policy;
DROP USER ${user_pol}, ${user_nopol};
"
