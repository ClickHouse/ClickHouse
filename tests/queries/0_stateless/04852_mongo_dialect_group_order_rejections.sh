#!/usr/bin/env bash
# The orders of a `$sort` that the accumulators of a following `$group` cannot be lowered through:
# keys that do not share one direction have no tuple to compare by, and keys that a stage in
# between left out of the documents it builds are not fields the `$group` can name. Both are an
# error rather than an arbitrary document of the group, which `any` would answer with.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS group_order_rejections;
    CREATE TABLE group_order_rejections (k String, ts Int64, v String) ENGINE = MergeTree ORDER BY (k, ts);
    INSERT INTO group_order_rejections VALUES ('a', 1, 'a1'), ('a', 2, 'a2'), ('a', 3, 'a3'), ('b', 1, 'b1'), ('b', 2, 'b2');
"

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- the keys of the sort do not share one direction'
run 'db.group_order_rejections.aggregate([{"$sort" : {"k" : 1, "ts" : -1}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}}}]);'
run 'db.group_order_rejections.aggregate([{"$sort" : {"k" : 1, "ts" : -1}}, {"$group" : {"_id" : "$k", "all" : {"$push" : "$v"}}}]);'

echo '-- a stage in between builds documents that do not have the sort key'
run 'db.group_order_rejections.aggregate([{"$sort" : {"ts" : 1}}, {"$project" : {"k" : 1, "v" : 1}}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}}}]);'

echo '-- an accumulator that does not depend on the order is unaffected'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 \
    --query 'db.group_order_rejections.aggregate([{"$sort" : {"k" : 1, "ts" : -1}}, {"$group" : {"_id" : "$k", "n" : {"$sum" : 1}}}, {"$sort" : {"_id" : 1}}]);'

echo '-- and so is a group that no sort precedes'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 \
    --query 'db.group_order_rejections.aggregate([{"$group" : {"_id" : "$k", "n" : {"$sum" : 1}}}, {"$sort" : {"_id" : 1}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE group_order_rejections"
