#!/usr/bin/env bash
# Tags: distributed
# A policy on a column the query does not select widens the read past the columns the user asked
# for. Only a cluster with a <secret> makes the receiving node run as the initial user rather than
# as the interserver default, so the grants of that user reach the re-planned read at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_X="rp_x_${CLICKHOUSE_DATABASE}"
USER_NONE="rp_none_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
DROP USER IF EXISTS ${USER_X}, ${USER_NONE};
DROP TABLE IF EXISTS rp_g_leaf;
DROP TABLE IF EXISTS rp_g_dist;

CREATE TABLE rp_g_leaf (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_g_leaf SELECT number, number FROM numbers(10);
CREATE TABLE rp_g_dist AS rp_g_leaf
    ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), rp_g_leaf);

DROP ROW POLICY IF EXISTS rp_g_policy ON rp_g_leaf;
CREATE ROW POLICY rp_g_policy ON rp_g_leaf FOR SELECT USING y < 5 TO ALL;

CREATE USER ${USER_X} IDENTIFIED WITH plaintext_password BY '';
GRANT SELECT(x) ON ${CLICKHOUSE_DATABASE}.rp_g_leaf TO ${USER_X};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.rp_g_dist TO ${USER_X};

CREATE USER ${USER_NONE} IDENTIFIED WITH plaintext_password BY '';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.rp_g_dist TO ${USER_NONE};
"

# Prints the summed value, or just the error name, so that an added grant shows up as a value and
# a missing one as ACCESS_DENIED.
run_as()
{
    ${CLICKHOUSE_CLIENT} --user "$1" --password '' \
        -q "SELECT sum(x) FROM rp_g_dist SETTINGS prefer_localhost_replica = 0, enable_analyzer = 1, serialize_query_plan = $2" 2>&1 \
        | grep -m1 -oE '^[0-9]+|ACCESS_DENIED'
}

# Granted only the selected column, while the policy reads another one: both settings must return
# the policy-filtered value. Returning 45 would mean the policy was skipped.
for sqp in 0 1; do
    echo "granted x, policy on y, sqp=${sqp} $(run_as "${USER_X}" "${sqp}")"
done

# No grant on the table the read resolves to: refused for either setting.
for sqp in 0 1; do
    echo "no grant on the leaf, sqp=${sqp} $(run_as "${USER_NONE}" "${sqp}")"
done

# Granted only a column the query does not select: refused for either setting.
${CLICKHOUSE_CLIENT} -m -q "
REVOKE SELECT(x) ON ${CLICKHOUSE_DATABASE}.rp_g_leaf FROM ${USER_X};
GRANT SELECT(y) ON ${CLICKHOUSE_DATABASE}.rp_g_leaf TO ${USER_X};
"
for sqp in 0 1; do
    echo "granted y only, selects x, sqp=${sqp} $(run_as "${USER_X}" "${sqp}")"
done

${CLICKHOUSE_CLIENT} -m -q "
DROP ROW POLICY rp_g_policy ON rp_g_leaf;
DROP TABLE rp_g_dist;
DROP TABLE rp_g_leaf;
DROP USER ${USER_X}, ${USER_NONE};
"

# The initiator lowers additional_table_filters into a filter step of the shipped plan, so
# resolving the setting again on the executing node would filter a second time. A deterministic
# filter cannot tell one application from two, so classify a nondeterministic one: it keeps about
# half the rows once and about a quarter twice. The filter's key has to name the database, which is
# why this arm lives in a shell test: a Map key is a literal, not an expression.
${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS rp_atf;
DROP TABLE IF EXISTS rp_atf_dist;
CREATE TABLE rp_atf (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_atf SELECT number, number FROM numbers(100000);
CREATE TABLE rp_atf_dist AS rp_atf
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_atf);
DROP ROW POLICY IF EXISTS rp_atf_policy ON rp_atf;
-- Keeps every row, so the count reflects only how often the setting's filter ran, while still
-- routing the read down the policy path.
CREATE ROW POLICY rp_atf_policy ON rp_atf FOR SELECT USING y < 100000 TO ALL;
"

for sqp in 1 0; do
    echo -n "atf nd sqp=${sqp} "
    # 100000 here would mean the filter matched no table and nothing was measured.
    ${CLICKHOUSE_CLIENT} -q "
        SELECT if(abs(count() - 50000) < 2000, 'ONCE', if(abs(count() - 25000) < 2000, 'TWICE', 'OTHER'))
        FROM rp_atf_dist
        SETTINGS prefer_localhost_replica = 0, enable_analyzer = 1, serialize_query_plan = ${sqp},
                 additional_table_filters = {'${CLICKHOUSE_DATABASE}.rp_atf': 'rand64() % 2 = 0'}"
done

${CLICKHOUSE_CLIENT} -m -q "
DROP ROW POLICY rp_atf_policy ON rp_atf;
DROP TABLE rp_atf_dist;
DROP TABLE rp_atf;
"
