#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test does not build the encryption functions

# a key expression can hold credentials too, and SHOW CREATE TABLE masks them. the copy needs SELECT
# on the source, unless this query gives its own key.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT TABLE ENGINE ON MergeTree TO ${user};

    CREATE TABLE ${db}.key_src (s String) ENGINE = MergeTree ORDER BY encrypt('aes-128-ecb', s, '0123456789abcdef');

    GRANT SHOW COLUMNS ON ${db}.key_src TO ${user};
"

# prints the missing privilege, or the sorting key of the copy
function try_copy()
{
    local name=$1 clauses=$2
    echo "-- ${name}:"
    ${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.${name} AS ${db}.key_src ${clauses}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT sorting_key FROM system.tables WHERE database = '${db}' AND name = '${name}'"
}

echo "with SHOW COLUMNS only:"
try_copy copy_of_key_src ""
try_copy own_key "ORDER BY s"

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.key_src TO ${user}"
try_copy copy_of_key_src ""

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
