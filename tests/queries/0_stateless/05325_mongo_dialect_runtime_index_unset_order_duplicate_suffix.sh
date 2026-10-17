#!/usr/bin/env bash
# - `$arrayElemAt` with an index that is computed at runtime counts from the end of the array when
#   the index is negative, the same way a negative constant index does.
# - `$unset` carries a preceding `$sort` key that it does not remove into an order-sensitive
#   `$group`, the same way an exclusion `$project` does.
# - A cursor suffix such as `.limit(...)` given more than once is an error rather than having all
#   but the first one silently dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_runtime_index;
    CREATE TABLE mongo_runtime_index (id Int32, a Array(Int32), idx Int32) ENGINE = Memory;
    INSERT INTO mongo_runtime_index VALUES (1, [10, 20, 30], 0), (2, [10, 20, 30], 1), (3, [10, 20, 30], -1), (4, [10, 20, 30], -3);

    DROP TABLE IF EXISTS mongo_unset_order;
    CREATE TABLE mongo_unset_order (k String, ts UInt8, v String, other UInt8) ENGINE = MergeTree ORDER BY (k, ts);
    INSERT INTO mongo_unset_order VALUES ('a', 1, 'a1', 0), ('a', 2, 'a2', 0), ('b', 1, 'b1', 0), ('b', 2, 'b2', 0);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1"
}

# Prints the error of a query without the parenthesised error name and the stack trace. The
# `DB::Exception: ` prefix is dropped, because the test runner rejects the word `Exception` in the
# standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- $arrayElemAt with a runtime index'
mongo 'db.mongo_runtime_index.aggregate([{"$project" : {"id" : 1, "at" : {"$arrayElemAt" : ["$a", "$idx"]}}}, {"$sort" : {"id" : 1}}]);'

echo '-- $unset keeps the order of the sort key it does not remove'
mongo 'db.mongo_unset_order.aggregate([{"$sort" : {"ts" : 1}}, {"$unset" : "other"}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}, "last" : {"$last" : "$v"}}}, {"$sort" : {"_id" : 1}}]);'
mongo 'db.mongo_unset_order.aggregate([{"$sort" : {"ts" : -1}}, {"$unset" : ["other", "v"]}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$ts"}}}, {"$sort" : {"_id" : 1}}]);'

echo '-- $unset of the sort key'
run 'db.mongo_unset_order.aggregate([{"$sort" : {"ts" : 1}}, {"$unset" : "ts"}, {"$group" : {"_id" : "$k", "first" : {"$first" : "$v"}}}]);'

echo '-- a repeated suffix'
run 'db.mongo_runtime_index.find({}).limit(5).limit(1);'
run 'db.mongo_runtime_index.find({}).skip(1).skip(2);'
run 'db.mongo_runtime_index.find({}).sort({"id" : 1}).limit(1).sort({"id" : -1});'
mongo 'db.mongo_runtime_index.find({}).sort({"id" : -1}).skip(1).limit(1);'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_runtime_index;
    DROP TABLE mongo_unset_order;
"
