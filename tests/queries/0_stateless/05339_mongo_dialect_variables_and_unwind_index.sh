#!/usr/bin/env bash
# A `$$<name>` of an aggregation expression is a variable bound by `$map` or `$filter`, never a
# column: it is refused outside the body of an operator that binds it. And the `includeArrayIndex`
# of `$unwind` creates the field it names or overwrites the field of that name the document has.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS unwind_index;
    CREATE TABLE unwind_index (_id String, a Array(Int64), idx String, missing Int64, e Int64) ENGINE = MergeTree ORDER BY _id;
    INSERT INTO unwind_index VALUES ('x', [10, 20], 'old', 7, 100), ('y', [], 'kept', 8, 200);
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 \
        | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/^Code: [0-9]*\. //' -e 's/DB::Exception: //' \
        | grep -v '^$'
}

echo '-- a bound variable'
mongo 'db.unwind_index.aggregate([{"$sort" : {"_id" : 1}}, {"$project" : {"_id" : 0, "m" : {"$map" : {"input" : "$a", "as" : "e", "in" : {"$add" : ["$$e", 1]}}}}}]);'
echo '-- an unbound variable is not a column'
mongo 'db.unwind_index.aggregate([{"$project" : {"x" : "$$missing"}}]);'
echo '-- a variable is bound only in the body'
mongo 'db.unwind_index.aggregate([{"$project" : {"m" : {"$map" : {"input" : "$a", "as" : "v", "in" : "$$missing"}}}}]);'
mongo 'db.unwind_index.aggregate([{"$project" : {"m" : {"$map" : {"input" : "$$e", "as" : "e", "in" : "$$e"}}}}]);'

echo '-- includeArrayIndex overwrites the field'
mongo 'db.unwind_index.aggregate([{"$unwind" : {"path" : "$a", "includeArrayIndex" : "idx"}}, {"$sort" : {"a" : 1}}, {"$project" : {"_id" : 0, "a" : 1, "idx" : 1}}]);'
echo '-- includeArrayIndex overwrites the field of a preserved document'
mongo 'db.unwind_index.aggregate([{"$unwind" : {"path" : "$a", "includeArrayIndex" : "idx", "preserveNullAndEmptyArrays" : true}}, {"$sort" : {"_id" : 1, "a" : 1}}, {"$project" : {"_id" : 1, "a" : 1, "idx" : 1}}]);'
echo '-- includeArrayIndex creates a new field'
mongo 'db.unwind_index.aggregate([{"$unwind" : {"path" : "$a", "includeArrayIndex" : "pos"}}, {"$sort" : {"a" : 1}}, {"$project" : {"_id" : 0, "a" : 1, "pos" : 1, "idx" : 1}}]);'
echo '-- includeArrayIndex naming the unwound field'
mongo 'db.unwind_index.aggregate([{"$unwind" : {"path" : "$a", "includeArrayIndex" : "a"}}]);'

${CLICKHOUSE_CLIENT} --query "DROP TABLE unwind_index"
