#!/usr/bin/env bash
# What the Mongo dialect does not support has to be an error naming the operator, and never a
# silently dropped condition or a wrong result. The whole surface SingleStore Kai lists but this
# dialect does not implement is checked here, together with the malformed arguments.
#
# Each query runs on its own rather than in a `.sql` file with `-- { clientError ... }` hints: a
# comment is part of the query text in the Mongo dialect, so an annotation would change the query
# it annotates.
#
# The arguments MongoDB itself rejects are taken from the integration suite of FerretDB, which
# compares itself against a real MongoDB server for each of them. Four of those inputs used to
# reach the rest of the server as a tree with a hole in it and crash it, so they are kept here as
# a regression test rather than only as a check of the wording of an error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS docs;
    CREATE TABLE docs (id Int32, name String, other String, tags Array(String)) ENGINE = Memory;
    INSERT INTO docs VALUES (1, 'alpha', '', ['red']);
"

# Prints the error of a query without the parenthesised error name and the stack trace, so that the
# message itself is what the reference records. The `DB::Exception: ` prefix is dropped as well,
# because the test runner rejects the word `Exception` in the standard output of a test.
run() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- filter operators'
run 'db.docs.find({"name" : {"$type" : "string"}});'
run 'db.docs.find({"name" : {"$typo" : 1}});'
run 'db.docs.find({"tags" : {"$elemMatch" : {"colour" : "red"}}});'
run 'db.docs.find({"id" : {"$in" : "not an array"}});'
run 'db.docs.find({"id" : {"$mod" : [2]}});'
run 'db.docs.find({"id" : {"$exists" : 1}});'
run 'db.docs.find({"id" : {}});'

echo '-- inputs that used to reach the rest of the server as a malformed tree'
run 'db.docs.find({"$and" : [{"id" : 1}, true]});'
run 'db.docs.find({"$or" : [{"id" : 1}, "string"]});'
run 'db.docs.find({"$nor" : [{"id" : 1}, 42]});'
run 'db.docs.find({"" : "foo"});'
run 'db.docs.find({"$or" : [{}, {}]});'
run 'db.docs.find({"$and" : [{"$comment" : "only a comment"}, {"id" : 1}]});'

echo '-- arguments MongoDB rejects'
run 'db.docs.find({"$or" : []});'
run 'db.docs.find({"$and" : []});'
run 'db.docs.find({"id" : {"$mod" : [0, 1]}});'
run 'db.docs.find({"id" : {"$bitsAllSet" : [1.2]}});'
run 'db.docs.find({"id" : {"$bitsAllSet" : [64]}});'
run 'db.docs.find({"id" : {"$bitsAllSet" : "123"}});'

echo '-- aggregation stages'
run 'db.docs.aggregate([{"$facet" : {"a" : []}}]);'
run 'db.docs.aggregate([{"$out" : "other"}]);'
run 'db.docs.aggregate([{"$lookup" : {"from" : "docs", "as" : "j"}}]);'
run 'db.docs.aggregate([{"$replaceWith" : "$tags"}]);'
run 'db.docs.aggregate([{"$unwind" : "tags"}]);'
run 'db.docs.aggregate([{"$unset" : []}]);'
run 'db.docs.aggregate([{"$sample" : {"n" : 2}}]);'
run 'db.docs.aggregate([{"$limit" : 0}]);'
run 'db.docs.aggregate([{"$limit" : -1}]);'
run 'db.docs.aggregate([{"$limit" : 2.5}]);'
run 'db.docs.aggregate([{"$skip" : -1}]);'
run 'db.docs.aggregate([{"$limit" : "5"}]);'
run 'db.docs.aggregate([{"$group" : {"c" : {"$sum" : 1}}}]);'
run 'db.docs.aggregate([{"$sort" : {"id" : 2}}]);'

echo '-- expression operators'
run 'db.docs.aggregate([{"$project" : {"x" : {"$sortArray" : {"input" : "$tags", "sortBy" : 1}}}}]);'
run 'db.docs.aggregate([{"$project" : {"x" : {"$function" : {"body" : "f", "args" : [], "lang" : "js"}}}}]);'
run 'db.docs.aggregate([{"$project" : {"x" : "$$ROOT"}}]);'
run 'db.docs.aggregate([{"$project" : {"x" : {"$trim" : {"input" : "$name", "chars" : "a"}}}}]);'
run 'db.docs.aggregate([{"$project" : {"x" : {"$size" : {"$regexFind" : {"input" : "$name", "regex" : "a"}}}}}]);'
run 'db.docs.aggregate([{"$group" : {"_id" : null, "x" : {"$mergeObjects" : "$name"}}}]);'
run 'db.docs.aggregate([{"$group" : {"_id" : null, "x" : {"$topN" : {"n" : 1}}}}]);'

echo '-- update operators'
run 'db.docs.updateMany({"id" : 1}, {"$setOnInsert" : {"id" : 1}});'
run 'db.docs.updateMany({"id" : 1}, {"$bit" : {"id" : {"and" : 1}}});'
run 'db.docs.updateMany({"id" : 1}, {"id" : 2});'
run 'db.docs.updateMany({"id" : 1}, {});'
run 'db.docs.updateMany({"id" : 1}, {"$pop" : {"tags" : 2}});'
run 'db.docs.updateMany({"id" : 1}, {"$rename" : {"name" : "name"}});'
run 'db.docs.updateMany({"id" : 1}, {"$rename" : {"name" : ""}});'
run 'db.docs.updateMany({"id" : 1}, {"$set" : {"id" : 1}, "$inc" : {"id" : 1}});'
run 'db.docs.updateMany({"id" : 1}, {"$rename" : {"name" : "other"}, "$unset" : {"other" : ""}});'

echo '-- the server is still healthy'
${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1 --query 'db.docs.find({"id" : 1});'

${CLICKHOUSE_CLIENT} --query "DROP TABLE docs"
