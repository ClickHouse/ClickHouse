#!/usr/bin/env bash
# Tags: shard

# Consuming a set-backed table through an `Alias` on the right of IN replaces reading the alias and
# the `StorageAlias::read` it would delegate to, so it requires SELECT on both the alias and the
# target, exactly like an ordinary alias on the right of IN does. The analyzer, the legacy IN
# implementation and the serialized-plan reconstruction all resolve the alias, so all of them check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS t_set;
    DROP TABLE IF EXISTS t_set_alias;
    DROP TABLE IF EXISTS t_src;

    CREATE TABLE t_set (a UInt8, b UInt8) ENGINE = Set;
    INSERT INTO t_set VALUES (1, 2);
    CREATE TABLE t_set_alias ENGINE = Alias('t_set');

    CREATE TABLE t_src (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_src VALUES (1, 2), (9, 9);

    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_src TO ${user};
    -- The serialized-plan case reads through a cluster, so it needs REMOTE as well. Granting it up
    -- front keeps every ACCESS_DENIED below attributable to the set-backed table alone.
    GRANT REMOTE ON *.* TO ${user};
"

run_all_paths()
{
    for analyzer in 1 0; do
        echo "  enable_analyzer = ${analyzer}"
        ${CLICKHOUSE_CLIENT} --user="${user}" --query \
            "SELECT (1, 2) IN t_set_alias SETTINGS enable_analyzer = ${analyzer}" 2>&1 \
            | grep -oE "ACCESS_DENIED|^[01]$" | uniq
    done
    # The plan is serialized to the shards, so the set is rebuilt from its table name there. Both
    # ends check, and the initiator checks first, so this asserts the outcome rather than which end
    # produced it: a single-node test cannot give the two ends different grants.
    echo "  serialized plan"
    ${CLICKHOUSE_CLIENT} --user="${user}" --query \
        "SELECT count() FROM cluster('test_cluster_two_shards', currentDatabase(), t_src)
         WHERE (a, b) IN t_set_alias
         SETTINGS serialize_query_plan = 1, enable_analyzer = 1, prefer_localhost_replica = 0" 2>&1 \
        | grep -oE "ACCESS_DENIED|^[0-9]+$" | uniq
}

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_set_alias TO ${user}"
echo "Alias granted, target not granted"
run_all_paths

${CLICKHOUSE_CLIENT} -m --query "
    REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t_set_alias FROM ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_set TO ${user};
"
echo "Target granted, alias not granted"
run_all_paths

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_set_alias TO ${user}"
echo "Both granted"
run_all_paths

# The whole set is consumed, so a column-level grant covering every column of the target is enough,
# and one that misses a column is not. An ordinary alias on the right of IN behaves the same way.
${CLICKHOUSE_CLIENT} -m --query "
    REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t_set FROM ${user};
    GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_set TO ${user};
"
echo "Target granted on some columns"
run_all_paths

${CLICKHOUSE_CLIENT} --query "GRANT SELECT(b) ON ${CLICKHOUSE_DATABASE}.t_set TO ${user}"
echo "Target granted on all columns"
run_all_paths

# The alias accepts a column-level grant covering every column as well.
${CLICKHOUSE_CLIENT} -m --query "
    REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t_set_alias FROM ${user};
    GRANT SELECT(a, b) ON ${CLICKHOUSE_DATABASE}.t_set_alias TO ${user};
"
echo "Alias granted on all columns"
run_all_paths

${CLICKHOUSE_CLIENT} -m --query "
    REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t_set_alias FROM ${user};
    GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_set_alias TO ${user};
"
echo "Alias granted on some columns"
run_all_paths

${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE t_src;
    DROP TABLE t_set_alias;
    DROP TABLE t_set;
    DROP USER ${user};
"
