#!/usr/bin/env bash
# The canonical Extended JSON form of a date is `{"$date": {"$numberLong": "<milliseconds>"}}`, and
# only that shape carries a value to convert: any other document names something else, so it is an
# error rather than a date read out of whatever its first member happens to be.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS dates;
    CREATE TABLE dates (id Int32, ts DateTime64(3, 'UTC')) ENGINE = Memory;
    INSERT INTO dates VALUES (1, '1970-01-01 00:00:00.000'), (2, '2019-01-01 00:00:00.000');
"

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- the document forms of `$date` that name no value'
run 'db.dates.find({"ts" : {"$date" : {}}});'
run 'db.dates.find({"ts" : {"$date" : {"oops" : 1}}});'
run 'db.dates.find({"ts" : {"$date" : {"$numberLong" : "0", "extra" : 1}}});'

echo '-- the canonical form, the legacy number and the relaxed string'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.dates.find({"ts" : {"$date" : {"$numberLong" : "0"}}});'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.dates.find({"ts" : {"$date" : 1546300800000}});'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.dates.find({"ts" : {"$date" : "2019-01-01"}});'

${CLICKHOUSE_CLIENT} --query "DROP TABLE dates"
