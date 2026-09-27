#!/usr/bin/env bash
# Tags: no-old-analyzer
# (the clamp is analyzer-path behaviour; the legacy interpreter throws on such a nested clause)
# A readonly user must not be able to override a locked setting through a SETTINGS clause nested in
# a subquery or in a view's inner query. The nested clause is clamped against the constraints: the
# violating changes are dropped and the query keeps working with the constraints enforced.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A user and a settings profile are server-global, not per-database, so their names carry the test
# database to keep this test safe against a concurrent copy of itself - which is how the flaky check
# runs it.
USER="user_05020_${CLICKHOUSE_DATABASE}"
PROFILE="profile_05020_${CLICKHOUSE_DATABASE}"
TABLE="t_05020"
VIEW="v_05020"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP USER IF EXISTS ${USER};
DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
DROP VIEW IF EXISTS ${CLICKHOUSE_DATABASE}.${VIEW};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE};

CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} (tenant_id UInt32, secret String) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO ${CLICKHOUSE_DATABASE}.${TABLE} VALUES (1, 'tenant1-own'), (2, 'tenant2-secret'), (3, 'tenant3-secret');

-- An administrator's view whose inner query carries a SETTINGS clause a readonly user cannot set.
-- Before the nested clause was clamped, reading it as a readonly user was impossible: the clause
-- either threw or, worse, was applied unchecked.
CREATE VIEW ${CLICKHOUSE_DATABASE}.${VIEW} SQL SECURITY INVOKER
    AS SELECT count() AS c FROM ${CLICKHOUSE_DATABASE}.${TABLE} SETTINGS max_execution_time = 5;

CREATE SETTINGS PROFILE ${PROFILE} SETTINGS
    readonly = 1 CONST,
    additional_table_filters = '{''${CLICKHOUSE_DATABASE}.${TABLE}'':''tenant_id = 1''}' CONST;
CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS PROFILE ${PROFILE};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER};
GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${USER};
"

RESTRICTED="${CLICKHOUSE_CLIENT_BINARY} --database=${CLICKHOUSE_DATABASE} --user=${USER}"

echo "-- the row filter is in force"
${RESTRICTED} --query "SELECT secret FROM ${TABLE} ORDER BY tenant_id"

echo "-- a nested SETTINGS clause cannot lift the row filter: it is dropped and the query still works"
${RESTRICTED} --query "SELECT secret FROM (SELECT * FROM ${TABLE} SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.${TABLE}':'1'}) ORDER BY secret"

echo "-- a readonly user can read a view whose inner query carries a SETTINGS clause"
${RESTRICTED} --query "SELECT c FROM ${VIEW}"

echo "-- the session is still readonly"
${RESTRICTED} --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.should_not_exist_05020 (x UInt64) ENGINE = MergeTree ORDER BY x" 2>&1 | grep -c -F "Cannot execute query in readonly mode"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE ${CLICKHOUSE_DATABASE}.${TABLE};
DROP VIEW ${CLICKHOUSE_DATABASE}.${VIEW};
DROP USER ${USER};
DROP SETTINGS PROFILE ${PROFILE};
"
