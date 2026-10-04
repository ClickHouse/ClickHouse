#!/usr/bin/env bash
# The Extended JSON wrappers that a top level constant accepts - `$numberInt`, `$numberLong`,
# `$numberDouble`, and the relaxed ISO 8601 string form of `$date` - are accepted inside an embedded
# document too, i.e. inside a value that becomes a `JSON` value: an element of an array or the
# document `$push` appends. Before, only `$numberDecimal`, `$oid` and the numeric forms of `$date`
# were converted there, and a nested `$numberLong` was an error.
#
# The malformed wrappers run on their own rather than with `-- { serverError ... }` hints: a comment
# is part of the query text in the Mongo dialect, so an annotation would change the query it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS numeric_wrappers;
    CREATE TABLE numeric_wrappers (id Int64, events Array(JSON)) ENGINE = MergeTree ORDER BY id;
"

MONGO="${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --mutations_sync 1 --session_timezone UTC"

${MONGO} --query 'db.numeric_wrappers.insertOne({"id": 1, "events": [{"n": {"$numberLong": "5000000000"}, "i": {"$numberInt": "-7"}, "d": {"$numberDouble": "2.5"}}]});'
${MONGO} --query 'db.numeric_wrappers.insertOne({"id": 2, "events": [{"n": {"$numberLong": 42}, "i": {"$numberInt": 8}, "d": {"$numberDouble": 1e300}}]});'
${MONGO} --query 'db.numeric_wrappers.insertOne({"id": 3, "events": [{"when": {"$date": "2019-01-01T00:00:00Z"}}, {"when": {"$date": "2019-01-01T12:30:00.250+02:00"}}]});'
${MONGO} --query 'db.numeric_wrappers.updateMany({"id": 3}, {"$push": {"events": {"n": {"$numberLong": "-1"}, "when": {"$date": "2020-02-29T23:59:59Z"}}}});'

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${MONGO} --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- malformed wrappers of an embedded document are an error, not a stored `$`-named field'
run 'db.numeric_wrappers.insertOne({"id": 4, "events": [{"n": {"$numberLong": "not a number"}}]});'
run 'db.numeric_wrappers.insertOne({"id": 4, "events": [{"n": {"$numberInt": "5000000000"}}]});'
run 'db.numeric_wrappers.insertOne({"id": 4, "events": [{"d": {"$numberDouble": "Infinity"}}]});'
run 'db.numeric_wrappers.insertOne({"id": 4, "events": [{"when": {"$date": "not a date"}}]});'

# The dates are shown in UTC, the zone they were written in. The `$push` is a mutation, which the
# server runs in its own time zone rather than the session's, and still stores the same instants.
echo '-- the stored documents'
${CLICKHOUSE_CLIENT} --session_timezone UTC --query "
    SELECT id, arrayMap(event -> JSONAllPaths(event), events) FROM numeric_wrappers ORDER BY id;
    SELECT id, arrayMap(event -> JSONAllPathsWithTypes(event), events) FROM numeric_wrappers ORDER BY id;
    SELECT id, events FROM numeric_wrappers ORDER BY id FORMAT JSONEachRow;
    DROP TABLE numeric_wrappers;
"
