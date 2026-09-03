#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ref_table="ref_${CLICKHOUSE_TEST_UNIQUE_NAME}"
data_table="data_${CLICKHOUSE_TEST_UNIQUE_NAME}"
dict_name="dict_${CLICKHOUSE_TEST_UNIQUE_NAME}"
view_name="view_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db_name="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP USER IF EXISTS ${user};
    DROP VIEW IF EXISTS ${view_name};
    DROP TABLE IF EXISTS ${data_table} SYNC;
    DROP DICTIONARY IF EXISTS ${dict_name};
    DROP TABLE IF EXISTS ${ref_table} SYNC;

    CREATE TABLE ${ref_table} (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${ref_table} VALUES (1, 'match'), (2, 'other');

    CREATE DICTIONARY ${dict_name} (id UInt64, name String)
    PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE '${ref_table}')) LAYOUT(HASHED_ARRAY()) LIFETIME(0);

    CREATE TABLE ${data_table} (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${data_table} VALUES (1), (1), (2);

    CREATE VIEW ${view_name} SQL SECURITY DEFINER AS
    SELECT id, dictGetString('${db_name}.${dict_name}', 'name', id) AS name FROM ${data_table};

    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${db_name}.${view_name} TO ${user};
"

# Sanity check: the user genuinely has no dictGet grant.
${CLICKHOUSE_CLIENT} --user="${user}" -nm --query "
    SELECT dictGetString('${db_name}.${dict_name}', 'name', 1); -- { serverError ACCESS_DENIED }
"

# Baseline: querying the definer view directly (no optimization) already works, since dictGet(...)
# runs under the view definer's rights, not the invoker's.
${CLICKHOUSE_CLIENT} --user="${user}" -nm --query "
    SELECT count() FROM ${db_name}.${view_name} WHERE name = 'match'
    SETTINGS enable_analyzer = 1, optimize_inverse_dictionary_lookup = 0;
"

# Regression check: with the optimization enabled, this used to throw ACCESS_DENIED because the
# rewrite fetched the dictionary under the invoker's (not the definer's) context.
${CLICKHOUSE_CLIENT} --user="${user}" -nm --query "
    SELECT count() FROM ${db_name}.${view_name} WHERE name = 'match'
    SETTINGS enable_analyzer = 1, optimize_inverse_dictionary_lookup = 1;
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP USER IF EXISTS ${user};
    DROP VIEW IF EXISTS ${view_name};
    DROP TABLE IF EXISTS ${data_table} SYNC;
    DROP DICTIONARY IF EXISTS ${dict_name};
    DROP TABLE IF EXISTS ${ref_table} SYNC;
"
