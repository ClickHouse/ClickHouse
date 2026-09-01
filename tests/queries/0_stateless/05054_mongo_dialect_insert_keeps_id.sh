#!/usr/bin/env bash
# An insert of the Mongo dialect writes a top level `_id` like every other field of the document.
# This parser has no schema of the target table, and dropping the identity of a document would be
# an irreversible loss, so a table that has an `_id` column keeps it, while a table that has none
# answers with an error rather than with an insert of the remaining fields.
#
# The error runs through the shell rather than through a `-- { serverError ... }` hint: a comment
# is part of the query text in the Mongo dialect, so an annotation would change the query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS with_id;
    DROP TABLE IF EXISTS without_id;
    CREATE TABLE with_id (\`_id\` String, a Int32) ENGINE = Memory;
    CREATE TABLE without_id (a Int32) ENGINE = Memory;
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query "$1"
}

# The message of the error names the table, whose database is the one of the test run, so the name
# of the error is what the reference records.
mongo_error() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query "$1" 2>&1 >/dev/null \
        | grep -oE '[A-Z_]+\)$' | tr -d ')'
}

echo '-- a table with an `_id` column keeps it'
mongo 'db.with_id.insertOne({"_id" : "1", "a" : 1});'
mongo 'db.with_id.find({});'

echo '-- a table without one is an error, and nothing is written'
mongo_error 'db.without_id.insertOne({"_id" : "1", "a" : 1});'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM without_id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE with_id; DROP TABLE without_id"
