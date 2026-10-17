#!/usr/bin/env bash
# The values an update statement cannot carry, and the regular expression options it cannot
# combine. An update statement is data rather than an aggregation pipeline, so a value that is
# neither a scalar, an array, nor a subdocument of them has nothing to be stored as; a field with
# no name is no column; and options next to a regular expression that carries options of its own
# are ambiguous, which MongoDB rejects as well.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS docs;
    CREATE TABLE docs (id Int32, name String, n Int32) ENGINE = Memory;
    INSERT INTO docs VALUES (1, 'alpha', 1);
"

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- the values an update operator cannot write'
run 'db.docs.updateMany({"id" : 1}, {"$inc" : {"n" : {"name" : 1}}});'
run 'db.docs.updateMany({"id" : 1}, {"$min" : {"n" : {"$unknownWrapper" : 1}}});'
run 'db.docs.updateMany({"id" : 1}, {"$set" : {"name" : {}}});'
run 'db.docs.updateMany({"id" : 1}, {"$set" : {"" : 1}});'
run 'db.docs.updateMany({"id" : 1}, {"$set" : {"name" : {"" : 1}}});'

echo '-- the regular expression options an operator cannot combine'
run 'db.docs.aggregate([{"$project" : {"x" : {"$regexMatch" : {"input" : "$name", "regex" : {"$regularExpression" : {"pattern" : "^a", "options" : "i"}}, "options" : "i"}}}}]);'
run 'db.docs.aggregate([{"$project" : {"x" : {"$regexMatch" : {"input" : "$name", "regex" : "^a", "options" : 1}}}}]);'
run 'db.docs.aggregate([{"$project" : {"x" : {"$regexMatch" : {"input" : "$name", "regex" : "^a", "options" : "z"}}}}]);'

echo '-- the server is still healthy'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.docs.find({"id" : 1});'

${CLICKHOUSE_CLIENT} --query "DROP TABLE docs"
