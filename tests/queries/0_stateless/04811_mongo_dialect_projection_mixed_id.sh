#!/usr/bin/env bash
# An inclusion projection may exclude `_id` and nothing else: `{"name": 1, "_id": 0}` is the
# usual way MongoDB clients ask for "only these fields", so it must work, while an exclusion of
# any other field inside an inclusion projection is an error in MongoDB - and `$project` used to
# drop such an exclusion silently instead of rejecting it.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS people;
    CREATE TABLE people (id Int32, name String, age Int64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO people VALUES (1, 'alpha', 30), (2, 'beta', 40);
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

echo '-- find: an inclusion may exclude _id'
run 'db.people.find({}, {"name" : 1, "_id" : 0}).sort({"name" : 1});'
run 'db.people.find({}, {"name" : 1, "age" : 1, "_id" : 0}).sort({"name" : 1});'
run 'db.people.find({}, {"_id" : 0}).sort({"name" : 1});'

echo '-- find: any other exclusion inside an inclusion is an error'
run_error 'db.people.find({}, {"name" : 1, "age" : 0});'
run_error 'db.people.find({}, {"name" : 1, "_id" : 0, "age" : 0});'

echo '-- $project: an inclusion may exclude _id'
run 'db.people.aggregate([{"$project" : {"name" : 1, "_id" : 0}}, {"$sort" : {"name" : 1}}]);'
run 'db.people.aggregate([{"$project" : {"_id" : 0, "age" : 0}}, {"$sort" : {"name" : 1}}]);'

echo '-- $project: any other exclusion inside an inclusion is an error'
run_error 'db.people.aggregate([{"$project" : {"name" : 1, "age" : 0}}]);'
run_error 'db.people.aggregate([{"$project" : {"name" : 1, "_id" : 0, "age" : 0}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE people"
