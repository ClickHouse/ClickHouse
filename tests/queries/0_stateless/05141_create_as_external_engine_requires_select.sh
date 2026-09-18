#!/usr/bin/env bash

# `CREATE TABLE x AS y` inherits the engine of `y` with its credentials, masked in `SHOW CREATE TABLE`.
# That needs SELECT on `y`; anything else still needs only SHOW COLUMNS.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
blind="blind_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

# The URLs are never contacted, only the definitions are copied.
${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user}, ${blind};
    CREATE USER ${user}, ${blind};
    GRANT CREATE TABLE ON ${db}.* TO ${user}, ${blind};
    GRANT TABLE ENGINE ON MergeTree, TABLE ENGINE ON Null TO ${user}, ${blind};
    GRANT URL ON *.* TO ${user}, ${blind};

    CREATE TABLE ${db}.local_src (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${db}.url_src (id UInt64) ENGINE = URL('http://user:password@127.0.0.1:1/', 'CSV');
    CREATE TABLE ${db}.url_src_no_password (id UInt64) ENGINE = URL('http://127.0.0.1:1/', 'CSV');
    CREATE TABLE ${db}.function_src (id UInt64) AS url('http://user:password@127.0.0.1:1/', 'CSV');
    CREATE TABLE ${db}.function_src_no_password (id UInt64) AS url('http://127.0.0.1:1/', 'CSV');

    GRANT SHOW COLUMNS ON ${db}.local_src TO ${user};
    GRANT SHOW COLUMNS ON ${db}.url_src TO ${user};
    GRANT SHOW COLUMNS ON ${db}.url_src_no_password TO ${user};
    GRANT SHOW COLUMNS ON ${db}.function_src TO ${user};
    GRANT SHOW COLUMNS ON ${db}.function_src_no_password TO ${user};
"

# Prints the missing privilege, or the engine of the copy. Further arguments go to the client.
function try_copy()
{
    local name=$1 source=$2
    shift 2
    echo "-- ${name}:"
    ${CLICKHOUSE_CLIENT} --user "${user}" "${@}" -q "CREATE TABLE ${db}.${name} AS ${db}.${source}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = '${name}'"
}

echo "with SHOW COLUMNS only:"
try_copy copy_of_local_src local_src
try_copy copy_of_url_src url_src
try_copy copy_of_function_src function_src
# Nothing is masked in these two, so they are copied as before.
try_copy copy_of_url_src_no_password url_src_no_password
try_copy copy_of_function_src_no_password function_src_no_password
# Both are replaced by `Null` here, so nothing masked is inherited.
try_copy null_copy_of_url_src url_src --restore_replace_external_engines_to_null 1
try_copy null_copy_of_function_src function_src --restore_replace_external_table_functions_to_null 1

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "
    GRANT SELECT ON ${db}.url_src TO ${user};
    GRANT SELECT ON ${db}.function_src TO ${user};
"
try_copy copy_of_url_src url_src
try_copy copy_of_function_src function_src

# A user who cannot see the source is told so, credentials or not.
echo "without SHOW COLUMNS:"
for src in local_src url_src
do
    echo "-- ${src}:"
    ${CLICKHOUSE_CLIENT} --user "${blind}" -q "CREATE TABLE ${db}.blind_copy AS ${db}.${src}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
done

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}, ${blind}"
