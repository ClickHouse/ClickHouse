#!/usr/bin/env bash
# The defaults of the date operators and what they ignore: `$dateToString` without a `format`
# writes ISO 8601 text that ends with `Z` in UTC only, a `startOfWeek` is ignored for a unit other
# than `week`, and the `amount` of `$dateAdd` must be a whole number rather than being truncated.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS date_defaults;
    CREATE TABLE date_defaults (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO date_defaults VALUES ('2024-03-09 23:30:00.123');
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

echo '-- the default format of $dateToString: with Z in UTC, without it in another time zone'
mongo 'db.date_defaults.aggregate([{"$project" : {"plain" : {"$dateToString" : {"date" : "$d"}}, "utc" : {"$dateToString" : {"date" : "$d", "timezone" : "UTC"}}, "zoned" : {"$dateToString" : {"date" : "$d", "timezone" : "America/New_York"}}}}]);'

echo '-- a startOfWeek is ignored for a unit other than week'
mongo 'db.date_defaults.aggregate([{"$project" : {"x" : {"$toLong" : {"$dateTrunc" : {"date" : "$d", "unit" : "day", "startOfWeek" : "wednesday"}}}, "y" : {"$toLong" : {"$dateTrunc" : {"date" : "$d", "unit" : "day"}}}}}]);'

echo '-- a whole amount is added, whether a literal or computed'
mongo 'db.date_defaults.aggregate([{"$project" : {"literal" : {"$toLong" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : 2.0}}}, "computed" : {"$toLong" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : {"$divide" : [4, 2]}}}}}}]);'

echo '-- what cannot be honoured is refused'
run 'db.date_defaults.aggregate([{"$project" : {"x" : {"$dateToString" : {"date" : "$d", "onNull" : "none"}}}}]);'
run 'db.date_defaults.aggregate([{"$project" : {"x" : {"$dateToString" : {"date" : "$d", "timezone" : "$zone"}}}}]);'
run 'db.date_defaults.aggregate([{"$project" : {"x" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : 1.5}}}}]);'
# The computed amount is checked per row, by the query; the message of the check is what matters.
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query 'db.date_defaults.aggregate([{"$project" : {"x" : {"$dateSubtract" : {"startDate" : "$d", "unit" : "day", "amount" : {"$divide" : [3, 2]}}}}}]);' 2>&1 \
    | grep -oF "The 'amount' of '\$dateSubtract' must be a whole number" | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE date_defaults"
