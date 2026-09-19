#!/usr/bin/env bash
# `find` is called positionally: an optional filter and an optional projection. The projection
# used to be dropped and a `find()` without arguments read past the argument list, so this test
# pins every arity down, together with the projection forms MongoDB defines: inclusion,
# exclusion, an empty document (the whole document), and the mix of the two (an error).
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS docs;
    CREATE TABLE docs (id Int32, name String, value Int64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO docs VALUES (1, 'alpha', 100), (2, 'beta', 200);
"

run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query "$1"
}

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run_error() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- the arities of find'
run 'db.docs.find();'
run 'db.docs.find({"id" : 2});'
run 'db.docs.find({"id" : 2}, {"name" : 1});'

echo '-- projections'
run 'db.docs.find({}, {"name" : 1, "value" : 1}).sort({"name" : 1});'
run 'db.docs.find({}, {"name" : true}).sort({"name" : 1});'
run 'db.docs.find({}, {"value" : 0}).sort({"name" : 1});'
run 'db.docs.find({}, {}).sort({"id" : 1});'

echo '-- rejected arguments'
run_error 'db.docs.find({}, {"name" : 1}, {"value" : 1});'
run_error 'db.docs.find("not a document");'
run_error 'db.docs.find({}, "not a document");'
run_error 'db.docs.find({}, {"name" : 1, "value" : 0});'
run_error 'db.docs.find({}, {"" : 1});'
run_error 'db.docs.aggregate();'

${CLICKHOUSE_CLIENT} --query "DROP TABLE docs"
