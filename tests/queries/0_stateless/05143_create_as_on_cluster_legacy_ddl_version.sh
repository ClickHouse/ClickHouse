#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `ON CLUSTER` is not allowed for a Replicated database.

# The older entry format ships the query as written and the worker materializes `AS src` with no user,
# so the initiator has to authorize the source, and the engine it brings along, itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
no_url="no_url_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    DROP USER IF EXISTS ${no_url};
    CREATE USER ${no_url};
    GRANT CREATE TABLE ON ${db}.* TO ${user}, ${no_url};
    GRANT CLUSTER ON *.* TO ${user}, ${no_url};
    GRANT URL ON *.* TO ${user};

    CREATE TABLE ${db}.url_src (id UInt64) ENGINE = URL('http://user:password@127.0.0.1:1/', 'CSV');
    CREATE TABLE ${db}.plain_src (id UInt64) ENGINE = MergeTree ORDER BY id;

    GRANT TABLE ENGINE ON MergeTree TO ${user};
    GRANT SHOW COLUMNS ON ${db}.url_src TO ${user};
    GRANT SHOW COLUMNS ON ${db}.plain_src TO ${user};
    GRANT SELECT ON ${db}.url_src TO ${no_url};
"

legacy=(--user "${user}" --distributed_ddl_output_mode throw --distributed_ddl_entry_format_version 2)
current=(--user "${user}" --distributed_ddl_output_mode throw)

# Prints why the copy was refused, or the engine of the copy. Each case uses its own destination name.
function try_copy()
{
    local name=$1 source=$2
    shift 2
    echo "-- ${name}:"
    ${CLICKHOUSE_CLIENT} "${@}" -q "CREATE TABLE ${db}.${name} ON CLUSTER test_shard_localhost AS ${source}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ([A-Za-z]+|${db}\.[a-z_]+)" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = '${name}'"
}

echo "with SHOW COLUMNS only:"
# Nothing is masked in this one, so it is copied without SELECT, as on the current version.
try_copy copy_legacy_plain "${db}.plain_src" "${legacy[@]}"
try_copy copy_legacy "${db}.url_src" "${legacy[@]}"
try_copy copy_current "${db}.url_src" "${current[@]}"
# An unqualified source is authorized, and read, in the database of this query.
try_copy copy_legacy_unqualified url_src "${legacy[@]}"

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.url_src TO ${user}"
try_copy copy_legacy "${db}.url_src" "${legacy[@]}"
try_copy copy_current "${db}.url_src" "${current[@]}"
try_copy copy_legacy_unqualified url_src "${legacy[@]}"

# The oldest version ships no settings, so the worker replaces nothing: a plain copy is still fine.
echo "with the oldest version and restore_replace_external_engines_to_null:"
try_copy copy_oldest_plain "${db}.plain_src" --user "${user}" --distributed_ddl_output_mode throw \
    --distributed_ddl_entry_format_version 1 --restore_replace_external_engines_to_null 1

# The engine comes along with the source, so it needs the grant for it as well.
echo "with SELECT but without the grant for the engine:"
try_copy copy_no_url "${db}.url_src" --user "${no_url}" --distributed_ddl_output_mode throw --distributed_ddl_entry_format_version 2

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}, ${no_url}"
