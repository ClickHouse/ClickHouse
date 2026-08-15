#!/usr/bin/env bash
# Tags: distributed, no-fasttest
# A policy on a column the query does not select widens the read past the columns the user asked
# for. Only a cluster with a <secret> makes the receiving node run as the initial user rather than
# as the interserver default, so the grants of that user reach the re-planned read at all. That
# cluster requires SSL, which the Fast test build does not have, hence no-fasttest.
# It also has two shards pointing at the same server, so the value arm aggregates per shard: it then
# asserts the policy alone rather than also depending on how many shards the cluster has.

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

# Prints one `shard<n>=<sum>` token per shard, or just the error name once, so that an added grant
# shows up as a value and a missing one as ACCESS_DENIED. A refusal is reported once per shard and
# how many shards report it before the query aborts is not deterministic, so deduplicate.
run_as()
{
    ${CLICKHOUSE_CLIENT} --user "$1" --password '' \
        -q "SELECT 'shard' || toString(_shard_num) || '=' || toString(sum(x)) FROM rp_g_dist
            GROUP BY _shard_num ORDER BY _shard_num
            SETTINGS prefer_localhost_replica = 0, enable_analyzer = 1, serialize_query_plan = $2" 2>&1 \
        | grep -oE '^shard[0-9]+=[0-9]+|ACCESS_DENIED' | awk '!seen[$0]++ {printf "%s%s", sep, $0; sep=" "}'
}

# Granted only the selected column, while the policy reads another one: both settings must return
# the policy-filtered value per shard. Returning 45 per shard would mean the policy was skipped.
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
