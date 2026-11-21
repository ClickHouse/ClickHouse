#!/usr/bin/env bash

# Tags: no-parallel-replicas

# A column referenced only inside `indexHint` is not read, but index analysis prunes granules by it,
# so `count()` acts as an oracle for its values. It must require the same SELECT grant as a regular predicate.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05293_${CLICKHOUSE_DATABASE}"
table="${CLICKHOUSE_DATABASE}.t_index_hint_access"

# The implicit minmax index on `id` would let `indexHint(id < 5)` prune granules.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS $table;
    CREATE TABLE $table (id UInt64, secret UInt64, secret_alias ALIAS secret)
    ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
    INSERT INTO $table SELECT number, number * 1000 FROM numbers(10);
    DROP USER IF EXISTS $user;
    CREATE USER $user;
    GRANT SELECT(id) ON $table TO $user;
"

function query_as_user()
{
    echo "--- ${1//${CLICKHOUSE_DATABASE}./}"
    ${CLICKHOUSE_CLIENT} --user "$user" --query "$1" 2>&1 | grep -oE '^[0-9]+$|ACCESS_DENIED' | uniq
}

echo "=== granted: id"
query_as_user "SELECT count() FROM $table WHERE id = 3 AND indexHint(secret > 5000)"
query_as_user "SELECT count() FROM $table WHERE id = 3 AND indexHint(secret < 5000)"
query_as_user "SELECT count() FROM $table WHERE indexHint(secret < 5000)"
query_as_user "SELECT count() FROM $table PREWHERE indexHint(secret < 5000)"
query_as_user "SELECT count() FROM (SELECT id FROM $table WHERE indexHint(secret < 5000))"
query_as_user "SELECT count() FROM $table WHERE id = 3 AND indexHint(secret_alias > 5000)"
query_as_user "SELECT count() FROM $table AS l JOIN $table AS r ON l.id = r.id WHERE indexHint(r.secret < 5000)"
query_as_user "SELECT count() FROM $table WHERE indexHint(id < 5)"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT(secret, secret_alias) ON $table TO $user"

echo "=== granted: id, secret, secret_alias"
query_as_user "SELECT count() FROM $table WHERE id = 3 AND indexHint(secret > 5000)"
query_as_user "SELECT count() FROM $table WHERE id = 3 AND indexHint(secret < 5000)"
query_as_user "SELECT count() FROM $table WHERE id = 3 AND indexHint(secret_alias > 5000)"

${CLICKHOUSE_CLIENT} --query "DROP USER $user; DROP TABLE $table"
