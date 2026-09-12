#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
# The UNIQUE KEY clause is unsupported on those database and storage flavors, so the tag set
# matches the other UNIQUE KEY DDL tests. Nothing is inserted here, so the dense-index SST is
# never written and RocksDB is not needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${user};

    SET allow_experimental_unique_key = 1;
    CREATE TABLE ${db}.uk_secret (id UInt64, hidden_col String)
    ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id);

    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON system.tables TO ${user};
    -- Column-scoped SELECT keeps the row visible while table-level SHOW COLUMNS stays false.
    GRANT SELECT(id) ON ${db}.uk_secret TO ${user};
"

unique_key_probe() {
    ${CLICKHOUSE_CLIENT} "$@" --query "
        SELECT notEmpty(unique_key) FROM system.tables
        WHERE database = '${db}' AND name = 'uk_secret';"
}

# Admin first: a 0 here means the table carries no unique key, and then every arm below would
# agree for the wrong reason.
echo "--- admin: unique_key present ---"
unique_key_probe

echo "--- column-scoped SELECT only: unique_key hidden ---"
unique_key_probe "--user=${user}"
${CLICKHOUSE_CLIENT} --user="${user}" --query "SHOW CREATE TABLE ${db}.uk_secret" 2>&1 | grep -o -m1 ACCESS_DENIED

echo "--- SHOW COLUMNS granted: unique_key visible ---"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${db}.uk_secret TO ${user};"
unique_key_probe "--user=${user}"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user};"
