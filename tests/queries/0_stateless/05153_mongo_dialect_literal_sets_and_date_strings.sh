#!/usr/bin/env bash
# A `$literal` of an array or of a document is a value, the way it is in MongoDB - that is the very
# reason the operator exists - a `$setDifference` answers a set rather than the left array with the
# common elements dropped, and a `$dateFromString` reads a text with no offset of its own in the
# time zone it names. A field of `$dateFromString` that answers a value of its own for a text that
# cannot be read, and an Extended JSON number that names no value of the width of its wrapper, are
# errors rather than a different value.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS literal_sets;
    CREATE TABLE literal_sets (a Array(Int32), b Array(Int32)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO literal_sets VALUES ([1, 1, 2], [2]);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1"
}

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- a literal array and a literal document are values'
mongo 'db.literal_sets.aggregate([{"$project" : {"array" : {"$literal" : [1, 2, 3]}, "document" : {"$literal" : {"a" : 1, "b" : {"c" : "d"}}}, "path" : {"$literal" : "$a"}}}]);'

echo '-- a set difference is a set'
mongo 'db.literal_sets.aggregate([{"$project" : {"difference" : {"$setDifference" : ["$a", "$b"]}}}]);'

echo '-- a date read in the time zone the operator names'
# The instant is what the time zone changes; the text a date prints in is the one of its own type,
# so each of them is answered as the number of seconds since the epoch instead.
mongo 'db.literal_sets.aggregate([{"$project" : {"utc" : {"$dateDiff" : {"startDate" : {"$date" : "1970-01-01T00:00:00Z"}, "endDate" : {"$dateFromString" : {"dateString" : "2026-01-01 00:00:00"}}, "unit" : "second"}}, "zoned" : {"$dateDiff" : {"startDate" : {"$date" : "1970-01-01T00:00:00Z"}, "endDate" : {"$dateFromString" : {"dateString" : "2026-01-01 00:00:00", "timezone" : "America/New_York"}}, "unit" : "second"}}, "formatted" : {"$dateDiff" : {"startDate" : {"$date" : "1970-01-01T00:00:00Z"}, "endDate" : {"$dateFromString" : {"dateString" : "2026-01-01 00:00:00", "format" : "%Y-%m-%d %H:%M:%S", "timezone" : "America/New_York"}}, "unit" : "second"}}}}]);'

echo '-- a field of the date operator that answers a value of its own'
run 'db.literal_sets.aggregate([{"$project" : {"d" : {"$dateFromString" : {"dateString" : "nonsense", "onError" : 0}}}}]);'

echo '-- an Extended JSON number outside the range of its wrapper'
run 'db.literal_sets.find({"a" : {"$numberInt" : 2147483648}});'
run 'db.literal_sets.find({"a" : {"$numberInt" : "-2147483649"}});'

${CLICKHOUSE_CLIENT} --query "DROP TABLE literal_sets;"
