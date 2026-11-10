#!/usr/bin/env bash
# A row policy whose USING expression contains a scalar subquery (here `has((SELECT [2]), tenant_id)`)
# is resolved through the analyzer's scalar-subquery cache, which lives in the same `Context::scalars`
# map that a client can write to with a `Scalar` packet (the `clickhouse-client --external ... --scalar`
# feature). A restricted user could therefore name a client scalar with the policy subquery's query-tree
# hash and forge its result, replacing `(SELECT [2])` with an attacker-chosen array and reading every
# tenant's rows. On 26.3 `clickhouse-client` cannot send scalars (no `--external ... --scalar`), so the
# forged-scalar cases are left out and this test only pins the policy baseline and that scalars shipped
# to shards in distributed queries still resolve.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# User and policy names are server-global, so suffix them with the (unique) test database.
user="u_scalar_rlp_${CLICKHOUSE_DATABASE}"
policy="rp_scalar_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id UInt64, tenant_id UInt8, secret String) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs SELECT number, number, 'secret-' || toString(number) FROM numbers(1, 8);

CREATE USER ${user} NOT IDENTIFIED;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.docs TO ${user};
-- The policy confines ${user} to tenant 2 only, via a scalar subquery.
CREATE ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.docs FOR SELECT USING has((SELECT [2]), tenant_id) TO ${user};
-- Once a table has any policy, \`throw_on_unmatched_row_policies\` (on in the test config) rejects
-- reads by users no policy matches, so the default user needs a permissive one of its own.
CREATE ROW POLICY ${policy}_all ON ${CLICKHOUSE_DATABASE}.docs FOR SELECT USING 1 TO ALL EXCEPT ${user};
"

echo "-- baseline: the policy confines the user to its single tenant (1)"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.docs SETTINGS enable_scalar_subquery_optimization = 1"

# Marking wire scalars as client-supplied must not disturb distributed queries: the initiator computes
# an array-returning scalar subquery once and ships the result to the shards in the same `Scalar` packet,
# where it arrives through `addClientScalar`. The shard consumes it through the explicit `__getScalar`
# reader, which still reads client-supplied entries, rather than through the narrowed subquery cache.
log_comment="scalar_rlp_dist_${CLICKHOUSE_DATABASE}"

echo "-- a scalar subquery shipped to the shards still resolves there (8 = 4 tenants x 2 shards)"
${CLICKHOUSE_CLIENT} --log_comment "${log_comment}" --query "
SELECT count() FROM cluster('test_cluster_two_shards', '${CLICKHOUSE_DATABASE}', docs)
WHERE has((SELECT [2, 3, 4, 5]), tenant_id)
SETTINGS prefer_localhost_replica = 0, enable_scalar_subquery_optimization = 1"

echo "-- and the shards consumed it through an explicit __getScalar reference (1)"
# The secondary queries log `current_database = 'default'` rather than the test database, so they are
# selected by the id of the initial query, which does carry it.
${CLICKHOUSE_CLIENT} --query "
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
  AND query LIKE '%__getScalar%'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 1
        AND current_database = currentDatabase() AND log_comment = '${log_comment}')"

${CLICKHOUSE_CLIENT} --query "
DROP ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.docs;
DROP ROW POLICY ${policy}_all ON ${CLICKHOUSE_DATABASE}.docs;
DROP TABLE docs;
"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
