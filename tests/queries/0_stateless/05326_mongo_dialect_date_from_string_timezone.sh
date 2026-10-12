#!/usr/bin/env bash
# `$dateFromString` reads a text without an offset in its `timezone`, and the result is the same
# instant printed in UTC like every other date of this dialect. `onError` and `onNull` change the
# result, so they are rejected rather than ignored, and so is an unknown argument.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_date_from_string_tz;
    CREATE TABLE mongo_date_from_string_tz (_id Int64, s String) ENGINE = Memory;
    INSERT INTO mongo_date_from_string_tz VALUES (1, '2024-01-01 00:00:00');
"

mongo() {
    ${CLICKHOUSE_CLIENT} --session_timezone 'Asia/Istanbul' --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1"
}

# Prints the error of a query without the parenthesised error name and the stack trace. The
# `DB::Exception: ` prefix is dropped, because the test runner rejects the word `Exception` in the
# standard output of a test.
run() {
    mongo "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

mongo 'db.mongo_date_from_string_tz.aggregate([{"$project" : {"utc" : {"$dateFromString" : {"dateString" : "$s"}}, "newYork" : {"$dateFromString" : {"dateString" : "$s", "timezone" : "America/New_York"}}, "newYorkFormatted" : {"$dateFromString" : {"dateString" : "$s", "format" : "%Y-%m-%d %H:%i:%S", "timezone" : "America/New_York"}}}}]);'

run 'db.mongo_date_from_string_tz.aggregate([{"$project" : {"d" : {"$dateFromString" : {"dateString" : "$s", "onNull" : 0}}}}]);'
run 'db.mongo_date_from_string_tz.aggregate([{"$project" : {"d" : {"$dateFromString" : {"dateString" : "$s", "onError" : 0}}}}]);'
run 'db.mongo_date_from_string_tz.aggregate([{"$project" : {"d" : {"$dateFromString" : {"dateString" : "$s", "typo" : 0}}}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_date_from_string_tz"
