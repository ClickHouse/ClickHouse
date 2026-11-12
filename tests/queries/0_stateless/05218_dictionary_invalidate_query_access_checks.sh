#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `invalidate_query` of a local `ClickHouse` dictionary source is executed as an `internal` query
# on behalf of the source user. Its text comes from `CREATE DICTIONARY`, so a user who may create a
# dictionary but not a table must not be able to smuggle a `CREATE TABLE` through it: only a `SELECT`
# is accepted, the same way as for the main dictionary query.

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT, CREATE DICTIONARY, DROP DICTIONARY, dictGet ON ${db}.* TO ${user}"

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${user} --password password"

# The test database may be reused across runs, so the objects of this test are dropped both before
# and after the run.
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS ${db}.dict_invalidate_ddl, ${db}.dict_invalidate_select"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.t_smuggled"

echo "-- a non-SELECT invalidate query of a local source is rejected, so it does not run as internal"
${CLIENT_AS_USER} --query "
    CREATE DICTIONARY ${db}.dict_invalidate_ddl (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(QUERY 'SELECT 1 AS id, 1 AS value' INVALIDATE_QUERY 'CREATE TABLE ${db}.t_smuggled (x UInt8) ENGINE = Memory'))
    LAYOUT(FLAT()) LIFETIME(MIN 1 MAX 1)
"
${CLIENT_AS_USER} --query "SELECT dictGet(${db}.dict_invalidate_ddl, 'value', toUInt64(1))"

# The first load does not run the invalidate query; it is run by the periodic check (every 5 seconds)
# once the lifetime has passed, and it decides whether the dictionary is reloaded. Wait for that
# check to happen: the rejected invalidate query counts as "modified", so the dictionary is reloaded
# and `last_successful_update_time` advances.
first_update=$(${CLICKHOUSE_CLIENT} --query "SELECT toUnixTimestamp(last_successful_update_time) FROM system.dictionaries WHERE database = '${db}' AND name = 'dict_invalidate_ddl'")
for _ in $(seq 1 120)
do
    update=$(${CLICKHOUSE_CLIENT} --query "SELECT toUnixTimestamp(last_successful_update_time) FROM system.dictionaries WHERE database = '${db}' AND name = 'dict_invalidate_ddl'")
    if [[ "${update}" -gt "${first_update}" ]]
    then
        break
    fi
    sleep 0.5
done
echo "-- reloaded after the invalidate query was evaluated"
${CLICKHOUSE_CLIENT} --query "SELECT toUnixTimestamp(last_successful_update_time) > ${first_update} FROM system.dictionaries WHERE database = '${db}' AND name = 'dict_invalidate_ddl'"
${CLIENT_AS_USER} --query "SELECT dictGet(${db}.dict_invalidate_ddl, 'value', toUInt64(1))"

echo "-- nothing was created"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${db}' AND name = 't_smuggled'"

echo "-- a SELECT invalidate query keeps working"
${CLIENT_AS_USER} --query "
    CREATE DICTIONARY ${db}.dict_invalidate_select (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(QUERY 'SELECT 1 AS id, 2 AS value' INVALIDATE_QUERY 'SELECT 1'))
    LAYOUT(FLAT()) LIFETIME(MIN 1 MAX 1)
"
${CLIENT_AS_USER} --query "SELECT dictGet(${db}.dict_invalidate_select, 'value', toUInt64(1))"

${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY ${db}.dict_invalidate_ddl, ${db}.dict_invalidate_select"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
