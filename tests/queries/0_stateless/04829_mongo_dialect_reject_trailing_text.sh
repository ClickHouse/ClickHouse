#!/usr/bin/env bash
# Text after a Mongo query that is not part of any recognized syntax has to be an error rather
# than silently dropped: `db.t.find({}) garbage` used to run as `db.t.find({})`, and a misspelled
# suffix such as `.limt(1)` used to silently drop the limit. The malformed inserts of the dialect
# `insertOne` / `insertMany` are checked here as well.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS docs;
    CREATE TABLE docs (id Int32, name String) ENGINE = Memory;
    INSERT INTO docs VALUES (1, 'alpha');
"

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- text after the query'
run 'db.docs.find({}) garbage;'
run 'db.docs.find({}).limit(1) oops;'
run 'db.docs.find({}).limt(1);'
run 'db.docs.find({}).explain();'
run 'db.docs.updateMany({"id" : 1}, {"$set" : {"id" : 2}}) tail;'
run 'db.docs.insertOne({"id" : 5, "name" : "e"}) x;'
run 'db.docs.deleteMany({"id" : 5}) and more;'

echo '-- the recognized suffixes and the argument list are not affected'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query 'db.docs.find({}).sort({"id" : 1}).skip(0).limit(1);'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query 'db.docs.find({"name" : "a) .limit("});'

echo '-- malformed inserts'
run 'db.docs.insertOne(5);'
run 'db.docs.insertOne({});'
run 'db.docs.insertMany([]);'
run 'db.docs.insertMany({"id" : 5});'
run 'db.docs.insertMany([{"id" : 5, "name" : "e"}, {"id" : 6}]);'
run 'db.docs.insertOne({"$id" : 5});'
run 'db.docs.insertOne({"" : 5});'

echo '-- the server is still healthy'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.docs.find({"id" : 1});'

${CLICKHOUSE_CLIENT} --query "DROP TABLE docs"
