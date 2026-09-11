#!/usr/bin/env bash
# The direction of a `.sort(...)` must be 1 or -1, as it is in MongoDB and in the `$sort`
# aggregation stage: `0`, another integer, a fraction or a string orders nothing, so it is a
# controlled error rather than a silently accepted or an unvalidated value.
#
# Each failing query runs on its own rather than in a `.sql` file with `-- { clientError ... }`
# hints: a comment is part of the query text in the Mongo dialect, so an annotation would change
# the query it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS docs;
    CREATE TABLE docs (id Int32, name String) ENGINE = Memory;
    INSERT INTO docs VALUES (1, 'alpha'), (2, 'beta');
"

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- the directions a sort cannot take'
run 'db.docs.find({}).sort({"id" : 0});'
run 'db.docs.find({}).sort({"id" : 5});'
run 'db.docs.find({}).sort({"id" : 1.5});'
run 'db.docs.find({}).sort({"id" : "asc"});'
run 'db.docs.find({}).sort({"name" : 1, "id" : -2});'

echo '-- the directions a sort takes'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.docs.find({}).sort({"id" : -1});'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.docs.find({}).sort({"name" : 1});'

${CLICKHOUSE_CLIENT} --query "DROP TABLE docs"
