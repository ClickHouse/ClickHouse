#!/usr/bin/env bash
# The members of the date operators that change what they answer: the `timezone` a day, a week or a
# month is counted in, the `startOfWeek` of a truncation or a difference in weeks, and the `binSize`
# of a truncation. A member that is asked for is honoured or refused, never dropped.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates. Dates are answered as seconds since the epoch, because the text a date prints in is
# the one of its own type and would hide the instant behind the time zone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS date_members;
    CREATE TABLE date_members (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();
    -- Saturday, 2024-03-09 23:30 UTC: 18:30 of the same day in New York, where the daylight saving
    -- time begins the next morning.
    INSERT INTO date_members VALUES ('2024-03-09 23:30:00');
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

echo '-- a day truncated in UTC starts on the 9th, in New York on the 9th at 05:00 UTC'
mongo 'db.date_members.aggregate([{"$project" : {"utc" : {"$toLong" : {"$dateTrunc" : {"date" : "$d", "unit" : "day"}}}, "zoned" : {"$toLong" : {"$dateTrunc" : {"date" : "$d", "unit" : "day", "timezone" : "America/New_York"}}}}}]);'

echo '-- a week starts on Sunday unless told otherwise: the 3rd, or Monday the 4th'
mongo 'db.date_members.aggregate([{"$project" : {"sunday" : {"$dateTrunc" : {"date" : "$d", "unit" : "week"}}, "monday" : {"$dateTrunc" : {"date" : "$d", "unit" : "week", "startOfWeek" : "monday"}}, "binned" : {"$dateTrunc" : {"date" : "$d", "unit" : "week", "binSize" : 1}}}}]);'

echo '-- a day added in New York is 23 hours long that night'
mongo 'db.date_members.aggregate([{"$project" : {"utc" : {"$toLong" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : 1}}}, "zoned" : {"$toLong" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : 1, "timezone" : "America/New_York"}}}, "back" : {"$toLong" : {"$dateSubtract" : {"startDate" : {"$date" : "2024-03-10T23:30:00Z"}, "unit" : "day", "amount" : 1, "timezone" : "America/New_York"}}}}}]);'

echo '-- a difference in days counts the midnights of the time zone: one in UTC, none in New York'
mongo 'db.date_members.aggregate([{"$project" : {"utc" : {"$dateDiff" : {"startDate" : "$d", "endDate" : {"$date" : "2024-03-10T00:30:00Z"}, "unit" : "day"}}, "zoned" : {"$dateDiff" : {"startDate" : "$d", "endDate" : {"$date" : "2024-03-10T00:30:00Z"}, "unit" : "day", "timezone" : "America/New_York"}}}}]);'

echo '-- a difference in weeks counts the starts of a week: Sunday the 10th is one, Monday the 11th is not yet'
mongo 'db.date_members.aggregate([{"$project" : {"sunday" : {"$dateDiff" : {"startDate" : "$d", "endDate" : {"$date" : "2024-03-10T12:00:00Z"}, "unit" : "week"}}, "monday" : {"$dateDiff" : {"startDate" : "$d", "endDate" : {"$date" : "2024-03-10T12:00:00Z"}, "unit" : "week", "startOfWeek" : "Monday"}}, "backwards" : {"$dateDiff" : {"startDate" : {"$date" : "2024-03-10T12:00:00Z"}, "endDate" : "$d", "unit" : "week"}}}}]);'

echo '-- what cannot be honoured is refused'
run 'db.date_members.aggregate([{"$project" : {"x" : {"$dateTrunc" : {"date" : "$d", "unit" : "day", "binSize" : 2}}}}]);'
run 'db.date_members.aggregate([{"$project" : {"x" : {"$dateTrunc" : {"date" : "$d", "unit" : "week", "startOfWeek" : "wednesday"}}}}]);'
run 'db.date_members.aggregate([{"$project" : {"x" : {"$dateDiff" : {"startDate" : "$d", "endDate" : "$d", "unit" : "week", "startOfWeek" : 1}}}}]);'
run 'db.date_members.aggregate([{"$project" : {"x" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : 1, "onNull" : 0}}}}]);'
run 'db.date_members.aggregate([{"$project" : {"x" : {"$dateTrunc" : {"date" : "$d", "unit" : "day", "collation" : {}}}}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE date_members"
