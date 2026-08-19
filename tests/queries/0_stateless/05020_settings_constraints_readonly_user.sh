#!/usr/bin/env bash
# A readonly user must not be able to leave readonly mode with `SET readonly = DEFAULT`,
# nor override a locked setting through a SETTINGS clause nested in a subquery.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER="user_05020"
PROFILE="profile_05020"
TABLE="t_05020"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP USER IF EXISTS ${USER};
DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE};

CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} (tenant_id UInt32, secret String) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO ${CLICKHOUSE_DATABASE}.${TABLE} VALUES (1, 'tenant1-own'), (2, 'tenant2-secret'), (3, 'tenant3-secret');

CREATE SETTINGS PROFILE ${PROFILE} SETTINGS
    readonly = 1 CONST,
    max_memory_usage = 104857600 CONST,
    additional_table_filters = '{''${CLICKHOUSE_DATABASE}.${TABLE}'':''tenant_id = 1''}' CONST;
CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS PROFILE ${PROFILE};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER};
GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${USER};
"

RESTRICTED="${CLICKHOUSE_CLIENT_BINARY} --database=${CLICKHOUSE_DATABASE} --user=${USER}"

echo "-- the row filter is in force"
${RESTRICTED} --query "SELECT secret FROM ${TABLE} ORDER BY tenant_id"

echo "-- SET readonly = DEFAULT is rejected"
${RESTRICTED} --query "SET readonly = DEFAULT" 2>&1 | grep -c -F "Cannot modify 'readonly' setting in readonly mode"

echo "-- the session is still readonly afterwards"
${RESTRICTED} --multiquery --ignore-error --query "SET readonly = DEFAULT; CREATE TABLE should_not_exist_05020 (x UInt64) ENGINE = MergeTree ORDER BY x" 2>&1 | grep -c -F "Cannot execute query in readonly mode"

echo "-- SET max_memory_usage = DEFAULT is rejected"
${RESTRICTED} --query "SET max_memory_usage = DEFAULT" 2>&1 | grep -c -F "Cannot modify 'max_memory_usage' setting in readonly mode"

echo "-- a nested SETTINGS clause cannot lift the row filter"
${RESTRICTED} --query "SELECT secret FROM (SELECT * FROM ${TABLE} SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.${TABLE}':'1'}) ORDER BY secret" 2>&1 | grep -c -F "Cannot modify 'additional_table_filters' setting in readonly mode"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE ${CLICKHOUSE_DATABASE}.${TABLE};
DROP USER ${USER};
DROP SETTINGS PROFILE ${PROFILE};
"
