#!/usr/bin/env bash
# Three behaviours of the `mongo` dialect:
# - `$ne`, `$nin`, `$not` and `$nor` match a document that has no value for the field, as in Mongo;
# - the arguments may be written as shell object literals, with bare keys and single quoted strings;
# - a stage between a `$sort` and a `$group` that keeps the sort keys does not prevent the
#   order-dependent accumulators of the `$group`, while one that replaces a sort key does.
#
# Each query runs on its own rather than in a `.sql` file: a comment is part of the query text in
# the Mongo dialect, so an annotation would change the query it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_negation;
    CREATE TABLE mongo_negation (id UInt64, a Nullable(Int64), tags Array(String)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_negation VALUES (1, 1, ['x']), (2, 2, ['y']), (3, NULL, []);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query "$1"
}

run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- $ne'
mongo 'db.mongo_negation.find({"a" : {"$ne" : 1}}, {"id" : 1}).sort({"id" : 1});'
echo '-- $nin'
mongo 'db.mongo_negation.find({"a" : {"$nin" : [1, 2]}}, {"id" : 1}).sort({"id" : 1});'
echo '-- $not'
mongo 'db.mongo_negation.find({"a" : {"$not" : {"$gt" : 1}}}, {"id" : 1}).sort({"id" : 1});'
echo '-- $nor'
mongo 'db.mongo_negation.find({"$nor" : [{"a" : {"$gt" : 1}}]}, {"id" : 1}).sort({"id" : 1});'
echo '-- $ne null keeps only the documents that have a value'
mongo 'db.mongo_negation.find({"a" : {"$ne" : null}}, {"id" : 1}).sort({"id" : 1});'

echo '-- shell object literals'
mongo "db.mongo_negation.find({a: {\$gte: 1}, tags: 'y'}, {id: 1, _id: 0});"
mongo "db.mongo_negation.find({ \$or : [ { id : 1 }, { id : 3 } ] }, { id : 1 }).sort({ id : -1 });"
mongo "db.mongo_negation.find({tags: 'don\\'t'}, {id: 1});"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_kept_sort_keys;
    CREATE TABLE mongo_kept_sort_keys (k String, ts Int64, v String) ENGINE = MergeTree ORDER BY (k, ts);
    INSERT INTO mongo_kept_sort_keys VALUES ('a', 1, 'a1'), ('a', 2, 'a2'), ('a', 3, 'a3'), ('b', 1, 'b1'), ('b', 2, 'b2');
"

echo '-- a $project that keeps the sort key'
mongo 'db.mongo_kept_sort_keys.aggregate([{"$sort" : {"ts" : 1}}, {"$project" : {"k" : 1, "ts" : 1, "v" : 1}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}, "last" : {"$last" : "$v"}}}, {"$sort" : {"_id" : 1}}]);'
echo '-- a $set of another field'
mongo 'db.mongo_kept_sort_keys.aggregate([{"$sort" : {"ts" : -1}}, {"$set" : {"w" : "$v"}}, {"$group" : {"_id" : "$k", "all" : {"$push" : "$w"}}}, {"$sort" : {"_id" : 1}}]);'
echo '-- a $set that replaces the sort key'
run 'db.mongo_kept_sort_keys.aggregate([{"$sort" : {"ts" : 1}}, {"$set" : {"ts" : 0}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}}}]);'
echo '-- a $project that renames the sort key'
run 'db.mongo_kept_sort_keys.aggregate([{"$sort" : {"ts" : 1}}, {"$project" : {"k" : 1, "v" : 1, "t" : "$ts"}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE mongo_negation; DROP TABLE mongo_kept_sort_keys;"
