#!/usr/bin/env bash

# Consuming a set-backed table through an `Alias` on the right of IN replaces reading it, so it
# requires SELECT on the target exactly like `StorageAlias::read` does. The analyzer, the legacy IN
# implementation and the serialized-plan path all resolve the alias, so all of them must check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS t_set;
    DROP TABLE IF EXISTS t_set_alias;

    CREATE TABLE t_set (arr Array(UInt8)) ENGINE = Set;
    INSERT INTO t_set VALUES ([1, 2, 3]);
    CREATE TABLE t_set_alias ENGINE = Alias('t_set');
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_set_alias TO ${user};
"

for analyzer in 1 0; do
    echo "Alias granted, target not granted, enable_analyzer = ${analyzer}"
    ${CLICKHOUSE_CLIENT} --user="${user}" --query \
        "SELECT [1, 2, 3] IN t_set_alias SETTINGS enable_analyzer = ${analyzer}" 2>&1 | grep -o "ACCESS_DENIED" | uniq
done

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_set TO ${user}"

for analyzer in 1 0; do
    echo "Target granted, enable_analyzer = ${analyzer}"
    ${CLICKHOUSE_CLIENT} --user="${user}" --query \
        "SELECT [1, 2, 3] IN t_set_alias AS present, [9, 9] IN t_set_alias AS absent SETTINGS enable_analyzer = ${analyzer}"
done

${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE t_set_alias;
    DROP TABLE t_set;
    DROP USER ${user};
"
