#!/usr/bin/env bash
# The direction of a `.sort(...)` is 1 or -1, the same as in a `$sort` stage of a pipeline:
# anything else has no meaning, and a value that is not an integer at all must not reach the
# `GetInt` of the JSON value, whose assertion aborts a debug build.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS docs;
    CREATE TABLE docs (id Int32, city String) ENGINE = Memory;
    INSERT INTO docs VALUES (1, 'alpha'), (2, 'beta');
"

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- the directions a sort accepts'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.docs.find({}).sort({"city" : 1});'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.docs.find({}).sort({"city" : -1});'

echo '-- the directions it does not'
run 'db.docs.find({}).sort({"city" : 0});'
run 'db.docs.find({}).sort({"city" : 2});'
run 'db.docs.find({}).sort({"city" : 1.5});'
run 'db.docs.find({}).sort({"city" : "asc"});'
run 'db.docs.find({}).sort({"city" : {}});'

${CLICKHOUSE_CLIENT} --query "DROP TABLE docs"
