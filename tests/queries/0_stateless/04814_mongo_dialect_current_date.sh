#!/usr/bin/env bash
# `$currentDate` accepts `true` and `{"$type": "date"}`, both of which write the current moment.
# `{"$type": "timestamp"}` asks for the BSON timestamp, a type that does not exist here, and
# anything else, such as `false`, is an error in MongoDB as well - neither may mutate the row.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS docs;
    CREATE TABLE docs (id Int32, seen DateTime64(3)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO docs VALUES (1, 0);
"

run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --mutations_sync 1 --query "$1"
}

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run_error() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- the legal forms write the current moment'
run 'db.docs.updateMany({"id" : 1}, {"$currentDate" : {"seen" : true}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, seen > '2026-01-01' FROM docs"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE docs UPDATE seen = 0 WHERE 1 SETTINGS mutations_sync = 1"
run 'db.docs.updateMany({"id" : 1}, {"$currentDate" : {"seen" : {"$type" : "date"}}});'
${CLICKHOUSE_CLIENT} --query "SELECT id, seen > '2026-01-01' FROM docs"

echo '-- the illegal forms are errors'
run_error 'db.docs.updateMany({"id" : 1}, {"$currentDate" : {"seen" : false}});'
run_error 'db.docs.updateMany({"id" : 1}, {"$currentDate" : {"seen" : {"$type" : "timestamp"}}});'
run_error 'db.docs.updateMany({"id" : 1}, {"$currentDate" : {"seen" : 5}});'
run_error 'db.docs.updateMany({"id" : 1}, {"$currentDate" : {"seen" : {"$type" : "unknown"}}});'

${CLICKHOUSE_CLIENT} --query "DROP TABLE docs"
