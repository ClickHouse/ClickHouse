#!/usr/bin/env bash
# A row policy whose USING expression contains a scalar subquery (here `has((SELECT [2]), tenant_id)`)
# is resolved through the analyzer's scalar-subquery cache, which lives in the same `Context::scalars`
# map that a client can write to with a `Scalar` packet (the `clickhouse-client --external ... --scalar`
# feature). A restricted user could therefore name a client scalar with the policy subquery's query-tree
# hash and forge its result, replacing `(SELECT [2])` with an attacker-chosen array and reading every
# tenant's rows. This test pins that the forged client scalar is ignored by the policy.

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

# Derive the query-tree hash the policy subquery `(SELECT [2])` is cached under. ClickHouse prints it
# itself as the argument of the internal `__getScalar` function (array/tuple scalars are rewritten to
# it), so no access to `system.row_policies` is needed - exactly the disclosure a real attacker uses.
# `enable_scalar_subquery_optimization` is pinned on: with it off the array subquery folds to a literal
# instead of `__getScalar('<hash>')`, so the hash would not be derivable and the repro would not run.
hash=$(${CLICKHOUSE_CLIENT} --query "EXPLAIN QUERY TREE SELECT has((SELECT [2]), materialize(1)) SETTINGS enable_scalar_subquery_optimization = 1" \
    | grep -A3 'function_name: __getScalar' | grep -oE "[0-9]+_[0-9]+" | head -1)

if [ -z "${hash}" ]; then echo "FAILED: could not derive scalar hash"; fi

# The scalar the attacker injects: an array naming every tenant, so a successful bypass would return all 8 rows.
scalar_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_scalar.tsv"
echo '[0,1,2,3,4,5,6,7,8]' > "${scalar_file}"

echo "-- baseline: the policy confines the user to its single tenant (1)"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.docs SETTINGS enable_scalar_subquery_optimization = 1"

echo "-- with a forged client scalar named as the policy subquery hash, the policy still holds (1)"
${CLICKHOUSE_CLIENT} --user "${user}" \
    --external --name="${hash}" --scalar --file="${scalar_file}" --structure="x Array(UInt8)" --format=TSV \
    --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.docs SETTINGS use_query_cache = 0, enable_scalar_subquery_optimization = 1"

echo "-- and the visible tenant is unchanged (2)"
${CLICKHOUSE_CLIENT} --user "${user}" \
    --external --name="${hash}" --scalar --file="${scalar_file}" --structure="x Array(UInt8)" --format=TSV \
    --query "SELECT DISTINCT tenant_id FROM ${CLICKHOUSE_DATABASE}.docs SETTINGS use_query_cache = 0, enable_scalar_subquery_optimization = 1"

echo "-- a client scalar is still usable through an explicit __getScalar reference (42)"
echo '42' > "${scalar_file}"
${CLICKHOUSE_CLIENT} --user "${user}" \
    --external --name="my_scalar" --scalar --file="${scalar_file}" --structure="x UInt64" --format=TSV \
    --query "SELECT __getScalar('my_scalar')"

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

rm -f "${scalar_file}"
${CLICKHOUSE_CLIENT} --query "
DROP ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.docs;
DROP ROW POLICY ${policy}_all ON ${CLICKHOUSE_DATABASE}.docs;
DROP TABLE docs;
"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
