#!/usr/bin/env bash
# An inclusion projection keeps `_id` unless it is excluded, in a `find` and in a `$project` stage,
# where the key of a `$group` comes through as well; a table without an `_id` column answers with
# the named fields only. The `n` of `$firstN` and `$lastN` is a positive whole number constant.
# A query read through a `Distributed` table reaches the shards as formatted text, so a literal of
# it has to have the type the shards read it back with.
#
# Each rejected query runs on its own rather than in a `.sql` file with `-- { clientError ... }`
# hints: a comment is part of the query text in the Mongo dialect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS implicit_id;
    DROP TABLE IF EXISTS no_id;
    DROP TABLE IF EXISTS no_id_distributed;
    CREATE TABLE implicit_id (_id String, name String, v Int64) ENGINE = MergeTree ORDER BY _id;
    INSERT INTO implicit_id VALUES ('a', 'x', 1), ('b', 'y', 2), ('c', 'x', 3);
    CREATE TABLE no_id (name String, v Int64) ENGINE = MergeTree ORDER BY name;
    INSERT INTO no_id VALUES ('x', 1), ('y', 2);
    CREATE TABLE no_id_distributed AS no_id ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), no_id);
"

MONGO_CLIENT="${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1"

run() {
    ${MONGO_CLIENT} --query "$1" 2>&1 >/dev/null \
        | head -1 | sed -e 's/^Received exception.*//' -e 's/ (version .*//' -e 's/\. ([A-Z_]*)$//' -e 's/DB::Exception: //'
}

echo '-- find keeps _id'
${MONGO_CLIENT} --query 'db.implicit_id.find({}, {"name" : 1}).sort({"_id" : 1});'
echo '-- find without _id'
${MONGO_CLIENT} --query 'db.implicit_id.find({}, {"name" : 1, "_id" : 0}).sort({"name" : 1, "v" : 1});'
echo '-- a table without _id'
${MONGO_CLIENT} --query 'db.no_id.find({}, {"v" : 1}).sort({"v" : 1});'
echo '-- $project keeps the key of $group'
${MONGO_CLIENT} --query 'db.implicit_id.aggregate([{"$group" : {"_id" : "$name", "s" : {"$sum" : "$v"}}}, {"$project" : {"s" : 1}}, {"$sort" : {"_id" : 1}}]);'
echo '-- $project without _id'
${MONGO_CLIENT} --query 'db.implicit_id.aggregate([{"$group" : {"_id" : "$name", "s" : {"$sum" : "$v"}}}, {"$project" : {"s" : 1, "_id" : 0}}, {"$sort" : {"s" : 1}}]);'

echo '-- $firstN'
${MONGO_CLIENT} --query 'db.no_id.aggregate([{"$sort" : {"v" : 1}}, {"$group" : {"_id" : null, "f" : {"$firstN" : {"input" : "$v", "n" : 1.0}}}}]);'
for n in '0' '-1' '1.5' '"$v"'; do
    echo "-- n = ${n}"
    run "db.no_id.aggregate([{\"\$group\" : {\"_id\" : null, \"f\" : {\"\$lastN\" : {\"input\" : \"\$v\", \"n\" : ${n}}}}}]);"
done

echo '-- a constant read through a Distributed table'
${MONGO_CLIENT} --prefer_localhost_replica 0 --query 'db.no_id_distributed.aggregate([{"$project" : {"name" : 1, "double" : {"$multiply" : ["$v", 2]}}}, {"$sort" : {"name" : 1}}]);'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE no_id_distributed;
    DROP TABLE no_id;
    DROP TABLE implicit_id;
"
