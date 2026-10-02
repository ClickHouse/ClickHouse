#!/usr/bin/env bash

# CREATE TABLE x AS y copies the engine of y with its credentials, which SHOW CREATE TABLE masks.
# That copy needs SELECT on y. Any other definition needs only SHOW COLUMNS.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
blind="blind_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

# the server does not contact the URLs. it only copies the definitions
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

# prints the missing privilege, or the engine of the copy. more arguments go to the client
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
# these two hold nothing masked, so the server copies them as before
try_copy copy_of_url_src_no_password url_src_no_password
try_copy copy_of_function_src_no_password function_src_no_password
# Null replaces both here, so the copy inherits nothing masked
try_copy null_copy_of_url_src url_src --restore_replace_external_engines_to_null 1
try_copy null_copy_of_function_src function_src --restore_replace_external_table_functions_to_null 1

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "
    GRANT SELECT ON ${db}.url_src TO ${user};
    GRANT SELECT ON ${db}.function_src TO ${user};
"
try_copy copy_of_url_src url_src
try_copy copy_of_function_src function_src

# a user who cannot see the source gets the same error for both tables
echo "without SHOW COLUMNS:"
for src in local_src url_src
do
    echo "-- ${src}:"
    ${CLICKHOUSE_CLIENT} --user "${blind}" -q "CREATE TABLE ${db}.blind_copy AS ${db}.${src}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
done

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}, ${blind}"
