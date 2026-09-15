#!/usr/bin/env bash

# `CREATE TABLE x AS y` inherits the engine of `y` with its credentials, which are masked in
# `SHOW CREATE TABLE`. That needs SELECT on `y`; copying any other definition still needs only SHOW COLUMNS.

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
    GRANT TABLE ENGINE ON MergeTree TO ${user}, ${blind};
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

# Prints either the missing privilege or the engine of the copy that was created.
function try_copy()
{
    echo "-- copy_of_${1}:"
    ${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.copy_of_${1} AS ${db}.${1}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'copy_of_${1}'"
}

echo "with SHOW COLUMNS only:"
try_copy local_src
try_copy url_src
try_copy function_src
# Nothing is masked in these two, so they are copied as before.
try_copy url_src_no_password
try_copy function_src_no_password

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "
    GRANT SELECT ON ${db}.url_src TO ${user};
    GRANT SELECT ON ${db}.function_src TO ${user};
"
try_copy url_src
try_copy function_src

# A user who may not see the source at all is told so, whether or not the definition holds credentials.
echo "without SHOW COLUMNS:"
for src in local_src url_src
do
    echo "-- ${src}:"
    ${CLICKHOUSE_CLIENT} --user "${blind}" -q "CREATE TABLE ${db}.blind_copy AS ${db}.${src}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
done

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}, ${blind}"
