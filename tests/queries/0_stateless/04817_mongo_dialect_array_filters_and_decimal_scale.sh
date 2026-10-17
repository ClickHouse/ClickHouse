#!/usr/bin/env bash
# Pins the array semantics of the filter operators and the scale of `$numberDecimal`:
# - `$in` and `$nin` on an array field test its elements, the way Mongo does, instead of
#   comparing the whole array against one candidate;
# - `$size` matches arrays only - a string of the right length is not a match, because to Mongo
#   `$size` on a non-array field matches no document;
# - a `$numberDecimal` keeps the scale of its value instead of being narrowed to a fixed
#   `Decimal128(10)`, and a value that fits no `Decimal128` is a controlled error;
# - a single query needs no trailing `;`, and a `;` inside a string literal is data, not a
#   statement boundary.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS docs;
    DROP TABLE IF EXISTS dec;
    CREATE TABLE docs (id Int32, name String, tags Array(String), nums Array(Int64)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO docs VALUES (1, 'alpha', ['red', 'blue'], [1, 2]), (2, 'a;b', ['green'], [3]), (3, 'gamma', [], []);
    CREATE TABLE dec (id Int32, d Decimal128(11)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO dec VALUES (1, toDecimal128('0.00000000001', 11)), (2, toDecimal128('1.5', 11));
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

echo '-- $in and $nin on an array field test its elements'
run 'db.docs.find({"tags" : {"$in" : ["red"]}}, {"id" : 1});'
run 'db.docs.find({"tags" : {"$nin" : ["red"]}}, {"id" : 1}).sort({"id" : 1});'
run 'db.docs.find({"nums" : {"$in" : [3, 4]}}, {"id" : 1});'

echo '-- $in and $nin on a scalar field'
run 'db.docs.find({"name" : {"$in" : ["alpha", "beta"]}}, {"id" : 1});'
run 'db.docs.find({"id" : {"$nin" : [1]}}, {"id" : 1}).sort({"id" : 1});'

echo '-- $size matches arrays only'
run 'db.docs.find({"tags" : {"$size" : 2}}, {"id" : 1});'
run 'db.docs.find({"nums" : {"$size" : 0}}, {"id" : 1});'
run 'db.docs.find({"name" : {"$size" : 5}}, {"id" : 1});'

echo '-- $numberDecimal keeps the scale of its value'
run 'db.dec.find({"d" : {"$eq" : {"$numberDecimal" : "0.00000000001"}}}, {"id" : 1});'
run 'db.dec.find({"d" : {"$eq" : {"$numberDecimal" : "1.50"}}}, {"id" : 1});'
run 'db.dec.find({"d" : {"$gt" : {"$numberDecimal" : "1E-20"}}}, {"id" : 1}).sort({"id" : 1});'

echo '-- a $numberDecimal that fits no Decimal128 is an error'
run_error 'db.dec.find({"d" : {"$eq" : {"$numberDecimal" : "1E+40"}}});'
run_error 'db.dec.find({"d" : {"$eq" : {"$numberDecimal" : "NaN"}}});'

echo '-- a single query needs no trailing semicolon'
run 'db.docs.find({"id" : 1}, {"name" : 1})'

echo '-- a semicolon inside a string is data, not a terminator'
run 'db.docs.find({"name" : "a;b"}, {"id" : 1});'
run 'db.docs.find({"name" : "a;b"}, {"id" : 1})'

echo '-- statements of a multi query are split at the terminator outside strings'
run 'db.docs.find({"name" : "a;b"}, {"id" : 1}); db.docs.find({"id" : 3}, {"id" : 1});'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE docs;
    DROP TABLE dec;
"
