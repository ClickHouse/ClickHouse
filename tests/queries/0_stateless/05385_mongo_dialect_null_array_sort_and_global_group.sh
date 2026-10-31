#!/usr/bin/env bash
# The order and the cardinality of what a Mongo query returns follow MongoDB:
# - `null` sorts below every other value, so it comes first ascending and last descending, in a
#   `find`, in `$sort` and in the order the accumulators of a `$group` read a sorted stream in;
# - a sort on an array is rejected, because MongoDB sorts an array by its smallest or largest
#   element, which is not the lexicographic order of ClickHouse;
# - `$sort` orders the output of the stage before it, even when that stage computed a field under
#   the name of a column of its source;
# - `$group` with a `_id` of `null` and `$count` produce one document for a nonempty stream and
#   none for an empty one;
# - `$first`, `$last`, `$push` and `$addToSet` keep a `null` value;
# - `$toInt` rejects a value out of the 32-bit range, and a malformed `$trim` is a controlled error.
#
# Each failing query runs on its own rather than in a `.sql` file with `-- { serverError ... }`
# hints: a comment is part of the query text in the Mongo dialect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_nulls;
    DROP TABLE IF EXISTS mongo_arrays;
    DROP TABLE IF EXISTS mongo_numbers;
    CREATE TABLE mongo_nulls (id Int32, ts Nullable(Int32), v Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_nulls VALUES (1, 1, NULL), (2, 2, 7), (3, NULL, 5);
    CREATE TABLE mongo_arrays (id Int32, a Array(Int32)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_arrays VALUES (1, [3, 10]), (2, [4, 1]);
    CREATE TABLE mongo_numbers (id Int32, x Int64, f Float64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_numbers VALUES (1, 1, 5.7), (2, 2, -5.7);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1"
}

# Prints the name of the error of a query.
err() {
    mongo "$1" 2>&1 >/dev/null | grep -o -m1 '([A-Z_]*)' | head -1
}

echo '-- null sorts first ascending and last descending'
mongo 'db.mongo_nulls.find({}, {"id" : 1}).sort({"v" : 1});'
mongo 'db.mongo_nulls.find({}, {"id" : 1}).sort({"v" : -1});'
mongo 'db.mongo_nulls.aggregate([{"$sort" : {"v" : 1}}, {"$limit" : 1}, {"$project" : {"id" : 1}}]);'
mongo 'db.mongo_nulls.aggregate([{"$sort" : {"v" : -1}}, {"$project" : {"id" : 1}}]);'

echo '-- a sort on an array is rejected'
err 'db.mongo_arrays.find({}).sort({"a" : 1});'
err 'db.mongo_arrays.aggregate([{"$sort" : {"a" : -1}}]);'
err 'db.mongo_arrays.aggregate([{"$sort" : {"a" : 1}}, {"$group" : {"_id" : null, "first" : {"$first" : "$id"}}}]);'

echo '-- $sort orders the output of the preceding stage'
mongo 'db.mongo_numbers.aggregate([{"$project" : {"x" : {"$multiply" : ["$x", -1]}}}, {"$sort" : {"x" : 1}}, {"$limit" : 1}]);'
mongo 'db.mongo_numbers.aggregate([{"$set" : {"x" : {"$multiply" : ["$x", -1]}}}, {"$sort" : {"x" : 1}}, {"$limit" : 1}, {"$project" : {"id" : 1}}]);'

echo '-- a global group is one document for a nonempty stream and none for an empty one'
mongo 'db.mongo_numbers.aggregate([{"$group" : {"_id" : null}}]);'
mongo 'db.mongo_numbers.aggregate([{"$group" : {"_id" : null, "s" : {"$sum" : "$x"}}}]);'
mongo 'db.mongo_numbers.aggregate([{"$match" : {"x" : 999}}, {"$group" : {"_id" : null, "s" : {"$sum" : "$x"}}}]);'
mongo 'db.mongo_numbers.aggregate([{"$match" : {"x" : 999}}, {"$group" : {"_id" : null}}]);'
mongo 'db.mongo_numbers.aggregate([{"$match" : {"x" : 999}}, {"$count" : "c"}]);'
mongo 'db.mongo_numbers.aggregate([{"$count" : "c"}]);'

echo '-- the accumulators keep null'
mongo 'db.mongo_nulls.aggregate([{"$match" : {"id" : {"$lt" : 3}}}, {"$sort" : {"ts" : 1}}, {"$group" : {"_id" : null, "first" : {"$first" : "$v"}, "last" : {"$last" : "$v"}}}]);'
mongo 'db.mongo_nulls.aggregate([{"$sort" : {"ts" : 1}}, {"$group" : {"_id" : null, "first" : {"$first" : "$v"}, "last" : {"$last" : "$v"}}}]);'
mongo 'db.mongo_nulls.aggregate([{"$sort" : {"ts" : -1}}, {"$group" : {"_id" : null, "first" : {"$first" : "$v"}, "pushed" : {"$push" : "$v"}}}]);'
mongo 'db.mongo_nulls.aggregate([{"$group" : {"_id" : null, "pushed" : {"$push" : "$v"}, "set" : {"$addToSet" : "$v"}}}, {"$project" : {"pushed" : {"$size" : "$pushed"}, "set" : {"$size" : "$set"}}}]);'
mongo 'db.mongo_nulls.aggregate([{"$match" : {"id" : 1}}, {"$group" : {"_id" : null, "first" : {"$first" : "$v"}, "last" : {"$last" : "$v"}, "firstN" : {"$firstN" : {"input" : "$v", "n" : 2}}}}]);'

echo '-- $toInt rejects a value out of the 32-bit range'
mongo 'db.mongo_numbers.aggregate([{"$match" : {"id" : 1}}, {"$project" : {"max" : {"$toInt" : 2147483647}, "min" : {"$toInt" : -2147483648}, "fraction" : {"$toInt" : "$f"}}}]);'
mongo 'db.mongo_numbers.aggregate([{"$match" : {"id" : 2}}, {"$project" : {"fraction" : {"$toInt" : "$f"}}}]);'
err 'db.mongo_numbers.aggregate([{"$project" : {"v" : {"$toInt" : 2147483648}}}]);'
err 'db.mongo_numbers.aggregate([{"$project" : {"v" : {"$toInt" : -2147483649}}}]);'
err 'db.mongo_numbers.aggregate([{"$project" : {"v" : {"$toInt" : {"$multiply" : ["$x", 3000000000]}}}}]);'

echo '-- a malformed $trim is a controlled error'
err 'db.mongo_numbers.aggregate([{"$project" : {"v" : {"$trim" : 1}}}]);'
err 'db.mongo_numbers.aggregate([{"$project" : {"v" : {"$ltrim" : "x"}}}]);'
err 'db.mongo_numbers.aggregate([{"$project" : {"v" : {"$rtrim" : [1]}}}]);'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_nulls;
    DROP TABLE mongo_arrays;
    DROP TABLE mongo_numbers;
"
