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
copy="CREATE TABLE ${db}.copy_of_url_src ON CLUSTER test_shard_localhost AS ${db}.url_src"

# Prints either the missing privilege or the engine of the copy that was created.
function try_copy()
{
    echo "-- ${1}:"
    shift
    ${CLICKHOUSE_CLIENT} "${@}" -q "${copy}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'copy_of_url_src'"
}

echo "with SHOW COLUMNS only:"
try_copy "the entry format version that ships the query as written" "${legacy[@]}"
try_copy "the current entry format version" "${current[@]}"

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.url_src TO ${user}"
try_copy "the entry format version that ships the query as written" "${legacy[@]}"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
