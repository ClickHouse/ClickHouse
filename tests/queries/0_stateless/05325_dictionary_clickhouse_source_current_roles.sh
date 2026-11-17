#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A local ClickHouse dictionary source runs its queries with the current default roles of the source user,
# like a new session of that user, not with the roles the user had when the dictionary was loaded.

db="${CLICKHOUSE_DATABASE}"
user="loader_${db}"
r_old="r_old_${db}"
r_new="r_new_${db}"
source_args="USER '${user}' PASSWORD 'p' DB '${db}' TABLE 'src'"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS ${db}.d_direct"
    ${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS ${db}.d_hashed"
    ${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS ${db}.d_invalidate"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
    ${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS ${r_old}, ${r_new}"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${db}.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO ${db}.src VALUES (1, 'a');
    CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'p' DEFAULT DATABASE ${db};
    CREATE ROLE ${r_old};
    GRANT SELECT ON ${db}.src TO ${r_old};
    GRANT ${r_old} TO ${user};

    CREATE DICTIONARY ${db}.d_direct (k UInt64, v String) PRIMARY KEY k
    SOURCE(CLICKHOUSE(${source_args})) LAYOUT(DIRECT());

    CREATE DICTIONARY ${db}.d_hashed (k UInt64, v String) PRIMARY KEY k
    SOURCE(CLICKHOUSE(${source_args})) LAYOUT(HASHED()) LIFETIME(MIN 1 MAX 1);

    CREATE DICTIONARY ${db}.d_invalidate (k UInt64, v String) PRIMARY KEY k
    SOURCE(CLICKHOUSE(${source_args} INVALIDATE_QUERY 'SELECT max(k) FROM ${db}.src')) LAYOUT(HASHED()) LIFETIME(MIN 1 MAX 1);
"

echo "-- loaded with the old role"
${CLICKHOUSE_CLIENT} --query "
    SELECT dictGet('${db}.d_direct', 'v', toUInt64(1)), dictGet('${db}.d_hashed', 'v', toUInt64(1)), dictGet('${db}.d_invalidate', 'v', toUInt64(1))
"

# Move the privilege to a new role. A new session of the user would start with the new role only.
${CLICKHOUSE_CLIENT} --query "
    CREATE ROLE ${r_new};
    GRANT SELECT ON ${db}.src TO ${r_new};
    GRANT ${r_new} TO ${user};
    DROP ROLE ${r_old};
    INSERT INTO ${db}.src VALUES (2, 'b');
"

echo "-- direct lookup uses the new role"
${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${db}.d_direct', 'v', toUInt64(2))"

# Periodic updates (and the invalidate query) are checked every 5 seconds.
get_updated="SELECT dictGetOrDefault('${db}.d_hashed', 'v', toUInt64(2), ''), dictGetOrDefault('${db}.d_invalidate', 'v', toUInt64(2), '')"
for _ in $(seq 1 120)
do
    [[ "$(${CLICKHOUSE_CLIENT} --query "${get_updated}")" == "$(printf 'b\tb')" ]] && break
    sleep 0.5
done
echo "-- periodic updates use the new role"
${CLICKHOUSE_CLIENT} --query "${get_updated}"

# A failed invalidate query does not leave stale data (the dictionary is then reloaded anyway), so check it in the log.
echo "-- no failed source or invalidate queries"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND user = '${user}' AND exception_code != 0
"

${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY ${db}.d_hashed"
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY ${db}.d_invalidate"

echo "-- no more roles than a new session: DEFAULT ROLE NONE"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} DEFAULT ROLE NONE"
${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${db}.d_direct', 'v', toUInt64(2))" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo "-- DEFAULT ROLE ALL"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} DEFAULT ROLE ALL"
${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${db}.d_direct', 'v', toUInt64(2))"

echo "-- role revoked"
${CLICKHOUSE_CLIENT} --query "REVOKE ${r_new} FROM ${user}"
${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${db}.d_direct', 'v', toUInt64(2))" 2>&1 | grep -o -m1 'ACCESS_DENIED'
