#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `ON CLUSTER` is not allowed for a Replicated database.

# Before `NORMALIZE_CREATE_ON_INITIATOR_VERSION` the entry ships the query text as written and the
# worker materializes `AS src` with no user, so the initiator, the last leg that runs as the real
# user, has to authorize the source table itself. Names are qualified because the worker resolves
# them in its own default database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT CLUSTER ON *.* TO ${user};
    GRANT URL ON *.* TO ${user};

    CREATE TABLE ${db}.url_src (id UInt64) ENGINE = URL('http://user:password@127.0.0.1:1/', 'CSV');

    GRANT SHOW COLUMNS ON ${db}.url_src TO ${user};
"

legacy=(--user "${user}" --distributed_ddl_output_mode throw --distributed_ddl_entry_format_version 2)
current=(--user "${user}" --distributed_ddl_output_mode throw)

# Prints either the missing privilege or the engine of the copy that was created. The name of the copy
# is derived from the destination so that the two source spellings do not collide.
function try_copy()
{
    local name=$1 source=$2
    shift 2
    echo "-- ${name}:"
    ${CLICKHOUSE_CLIENT} "${@}" -q "CREATE TABLE ${db}.${name} ON CLUSTER test_shard_localhost AS ${source}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = '${name}'"
}

echo "with SHOW COLUMNS only:"
try_copy copy_legacy "${db}.url_src" "${legacy[@]}"
try_copy copy_current "${db}.url_src" "${current[@]}"
# An unqualified source is authorized, and read, in the database of this query.
try_copy copy_legacy_unqualified url_src "${legacy[@]}"

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.url_src TO ${user}"
try_copy copy_legacy "${db}.url_src" "${legacy[@]}"
try_copy copy_legacy_unqualified url_src "${legacy[@]}"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
