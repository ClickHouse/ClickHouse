#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a Replicated database does not allow ON CLUSTER

# the older entry format sends the query as written and the worker builds AS src with no user.
# the initiator must check the source, and the engine that comes with it, itself.

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

# prints the missing privilege, or the engine of the copy. each case uses its own name
function try_copy()
{
    local name=$1 source=$2
    shift 2
    echo "-- ${name}:"
    ${CLICKHOUSE_CLIENT} "${@}" -q "CREATE TABLE ${db}.${name} ON CLUSTER test_shard_localhost AS ${source}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ([A-Za-z0-9_]+|${db}\.[a-z_]+)" | head -n 1 | sed "s/creds_${db}/creds/;s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = '${name}'"
}

echo "with SHOW COLUMNS only:"
# this one holds nothing masked, so the copy needs no SELECT, as on the current version
try_copy copy_legacy_plain "${db}.plain_src" "${legacy[@]}"
try_copy copy_legacy "${db}.url_src" "${legacy[@]}"
try_copy copy_current "${db}.url_src" "${current[@]}"
# the server checks and reads a source without a database name in the database of this query
try_copy copy_legacy_unqualified url_src "${legacy[@]}"

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.url_src TO ${user}"
try_copy copy_legacy "${db}.url_src" "${legacy[@]}"
try_copy copy_current "${db}.url_src" "${current[@]}"
try_copy copy_legacy_unqualified url_src "${legacy[@]}"

# the oldest version sends no settings, so the worker replaces nothing and a plain copy still works
echo "with the oldest version and restore_replace_external_engines_to_null:"
try_copy copy_oldest_plain "${db}.plain_src" --user "${user}" --distributed_ddl_output_mode throw \
    --distributed_ddl_entry_format_version 1 --restore_replace_external_engines_to_null 1

# the collection that the engine uses needs its grant too, on both entry formats
${CLICKHOUSE_CLIENT} -q "
    DROP NAMED COLLECTION IF EXISTS creds_${db};
    CREATE NAMED COLLECTION creds_${db} AS url = 'http://127.0.0.1:1/', format = 'CSV';
    CREATE TABLE ${db}.collection_src (id UInt64) ENGINE = URL(creds_${db});
    GRANT SHOW COLUMNS ON ${db}.collection_src TO ${user};
    GRANT SELECT ON ${db}.collection_src TO ${user};
"
echo "without the grant for the collection:"
try_copy copy_collection_legacy "${db}.collection_src" "${legacy[@]}"
try_copy copy_collection_current "${db}.collection_src" "${current[@]}"

# the engine comes with the source, so the copy needs the grant for the engine too
echo "with SELECT but without the grant for the engine:"
try_copy copy_no_url "${db}.url_src" --user "${no_url}" --distributed_ddl_output_mode throw --distributed_ddl_entry_format_version 2

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${db}.collection_src; DROP NAMED COLLECTION creds_${db}; DROP USER ${user}, ${no_url}"
