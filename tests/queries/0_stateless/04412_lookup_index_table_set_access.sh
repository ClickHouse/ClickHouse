#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"

${CLICKHOUSE_CLIENT} -nm --query "
    SET allow_experimental_lookup_index = 1;

    CREATE TABLE ${db}.lookup_acc_dim
    (
        id UInt64,
        LOOKUP INDEX idx_set (id) TYPE table_set
    )
    ENGINE = MergeTree
    ORDER BY id;

    CREATE TABLE ${db}.lookup_acc_dim_extra
    (
        id UInt64,
        tag String,
        LOOKUP INDEX idx_set (id) TYPE table_set
    )
    ENGINE = MergeTree
    ORDER BY id;

    CREATE TABLE ${db}.lookup_acc_fact
    (
        id UInt64,
        payload String
    )
    ENGINE = MergeTree
    ORDER BY id;

    INSERT INTO ${db}.lookup_acc_dim VALUES (1), (3);
    INSERT INTO ${db}.lookup_acc_dim_extra VALUES (1, 'a'), (3, 'b');
    INSERT INTO ${db}.lookup_acc_fact VALUES (1, 'x'), (2, 'y'), (3, 'z'), (5, 'w');

    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${db}.lookup_acc_fact TO ${user};
"

# The user can read the fact table but has no SELECT on the dimension tables.
# Using a dimension table on the right-hand side of IN must require SELECT on it,
# regardless of whether the table_set lookup index fast path is enabled. The lookup
# fast path must not let the user probe the dimension table's keys without access.
#
# The fast path only fires when no IN-set size limit is active, so `max_rows_in_set`
# and `max_bytes_in_set` are reset to 0 here (the standard test config sets them to a
# high non-zero ceiling, which would otherwise make the fast path decline).

echo 'direct IN, lookup ON (no access):'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT id FROM ${db}.lookup_acc_fact WHERE id IN ${db}.lookup_acc_dim ORDER BY id
    SETTINGS allow_experimental_lookup_index = 1, max_rows_in_set = 0, max_bytes_in_set = 0; -- { serverError ACCESS_DENIED }
"

echo 'subquery IN, lookup ON (no access):'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT id FROM ${db}.lookup_acc_fact WHERE id IN (SELECT id FROM ${db}.lookup_acc_dim_extra) ORDER BY id
    SETTINGS allow_experimental_lookup_index = 1, max_rows_in_set = 0, max_bytes_in_set = 0; -- { serverError ACCESS_DENIED }
"

# Same queries with the lookup index disabled must be denied as well (parity).
echo 'direct IN, lookup OFF (no access):'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT id FROM ${db}.lookup_acc_fact WHERE id IN ${db}.lookup_acc_dim ORDER BY id
    SETTINGS allow_experimental_lookup_index = 0; -- { serverError ACCESS_DENIED }
"

echo 'subquery IN, lookup OFF (no access):'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT id FROM ${db}.lookup_acc_fact WHERE id IN (SELECT id FROM ${db}.lookup_acc_dim_extra) ORDER BY id
    SETTINGS allow_experimental_lookup_index = 0; -- { serverError ACCESS_DENIED }
"

# After granting SELECT on the dimension tables, the lookup fast path works as usual.
${CLICKHOUSE_CLIENT} -nm --query "
    GRANT SELECT ON ${db}.lookup_acc_dim TO ${user};
    GRANT SELECT ON ${db}.lookup_acc_dim_extra TO ${user};
"

echo 'direct IN, lookup ON (with access):'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT id FROM ${db}.lookup_acc_fact WHERE id IN ${db}.lookup_acc_dim ORDER BY id
    SETTINGS allow_experimental_lookup_index = 1, max_rows_in_set = 0, max_bytes_in_set = 0;
"

echo 'subquery IN, lookup ON (with access):'
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT id FROM ${db}.lookup_acc_fact WHERE id IN (SELECT id FROM ${db}.lookup_acc_dim_extra) ORDER BY id
    SETTINGS allow_experimental_lookup_index = 1, max_rows_in_set = 0, max_bytes_in_set = 0;
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS ${db}.lookup_acc_dim SYNC;
    DROP TABLE IF EXISTS ${db}.lookup_acc_dim_extra SYNC;
    DROP TABLE IF EXISTS ${db}.lookup_acc_fact SYNC;
"
