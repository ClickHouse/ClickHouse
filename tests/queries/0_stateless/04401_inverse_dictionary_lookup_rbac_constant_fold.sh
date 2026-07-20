#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the optimization-off query evaluates `dictGetString` on workers, where the dictionary is not available.

# A user with `dictGet` access but without the `CREATE TEMPORARY TABLE` grant must still
# get the `dictGetKeys` constant fold (`key = const` / `key IN [..]`), because that path
# does not build a `dictionary()` table-function subquery and only needs the normal
# dictionary access of `dictGetKeys`. Only the fallback `IN (SELECT ... FROM dictionary(...))`
# rewrite needs `CREATE TEMPORARY TABLE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
table_name="table_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ref_name="ref_${CLICKHOUSE_TEST_UNIQUE_NAME}"
dict_name="dict_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db_name="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP USER IF EXISTS ${user};
    DROP DICTIONARY IF EXISTS ${dict_name};
    DROP TABLE IF EXISTS ${table_name} SYNC;
    DROP TABLE IF EXISTS ${ref_name} SYNC;

    CREATE TABLE ${ref_name} (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${ref_name} VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green');

    CREATE DICTIONARY ${dict_name} (id UInt64, name String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE '${ref_name}' DB '${db_name}'))
    LAYOUT(HASHED())
    LIFETIME(0);

    CREATE TABLE ${table_name} (color_id UInt64, payload String) ENGINE = MergeTree ORDER BY color_id;
    INSERT INTO ${table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${db_name}.${table_name} TO ${user};
    GRANT dictGet ON ${db_name}.${dict_name} TO ${user};
"

# The user can read the dictionary via dictGet, but cannot use the dictionary() table function.
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT count() FROM dictionary('${db_name}.${dict_name}'); -- { serverError ACCESS_DENIED }
"

echo '-- multi-key fold (IN): EXPLAIN must not contain dictGet or a dictionary() subquery'
plan_in=$(${CLICKHOUSE_CLIENT} --user="${user}" --query "
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT color_id FROM ${db_name}.${table_name}
    WHERE dictGetString('${db_name}.${dict_name}', 'name', color_id) = 'red'
    ORDER BY color_id
    SETTINGS enable_analyzer = 1, optimize_inverse_dictionary_lookup = 1")
echo "dictGet occurrences in plan (expect 0): $(echo "$plan_in" | grep -c dictGet)"
echo "dictionary() subquery occurrences in plan (expect 0): $(echo "$plan_in" | grep -c 'dictionary(')"

echo '-- multi-key fold (IN): result with optimization on'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT color_id FROM ${db_name}.${table_name}
    WHERE dictGetString('${db_name}.${dict_name}', 'name', color_id) = 'red'
    ORDER BY color_id
    SETTINGS enable_analyzer = 1, optimize_inverse_dictionary_lookup = 1"

echo '-- multi-key fold (IN): result with optimization off (must match)'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT color_id FROM ${db_name}.${table_name}
    WHERE dictGetString('${db_name}.${dict_name}', 'name', color_id) = 'red'
    ORDER BY color_id
    SETTINGS enable_analyzer = 1, optimize_inverse_dictionary_lookup = 0"

echo '-- single-key fold (=): EXPLAIN must not contain dictGet or a dictionary() subquery'
plan_eq=$(${CLICKHOUSE_CLIENT} --user="${user}" --query "
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT color_id FROM ${db_name}.${table_name}
    WHERE dictGetString('${db_name}.${dict_name}', 'name', color_id) = 'blue'
    ORDER BY color_id
    SETTINGS enable_analyzer = 1, optimize_inverse_dictionary_lookup = 1")
echo "dictGet occurrences in plan (expect 0): $(echo "$plan_eq" | grep -c dictGet)"
echo "dictionary() subquery occurrences in plan (expect 0): $(echo "$plan_eq" | grep -c 'dictionary(')"

echo '-- single-key fold (=): result'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT color_id FROM ${db_name}.${table_name}
    WHERE dictGetString('${db_name}.${dict_name}', 'name', color_id) = 'blue'
    ORDER BY color_id
    SETTINGS enable_analyzer = 1, optimize_inverse_dictionary_lookup = 1"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP USER IF EXISTS ${user};
    DROP DICTIONARY IF EXISTS ${dict_name};
    DROP TABLE IF EXISTS ${table_name} SYNC;
    DROP TABLE IF EXISTS ${ref_name} SYNC;
"
