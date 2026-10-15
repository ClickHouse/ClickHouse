#!/usr/bin/env bash
# - `$rename` of a subdocument replaces the target as a whole: the leaves of the target that the
#   source does not have go back to their defaults;
# - an `updateMany` of a user without access to the table is denied without revealing its columns;
# - the nesting of a query is bounded by `max_parser_depth`;
# - a query sent to the server with leading whitespace addresses the same collection;
# - the date formats of `$dateToString` and `$dateFromString` are MongoDB's, and the options of
#   `$dateTrunc` that change the result are honoured or rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05386_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mongo_rename;
    DROP TABLE IF EXISTS mongo_secret;
    DROP TABLE IF EXISTS mongo_dates;
    DROP USER IF EXISTS ${user};
    CREATE TABLE mongo_rename (id Int32, \`profile.name\` String, \`user.name\` String, \`user.age\` Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_rename VALUES (1, 'alpha', 'old', 99);
    CREATE TABLE mongo_secret (id Int32, \`profile.name\` String, \`profile.secret\` Int32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_secret VALUES (1, 'a', 1);
    CREATE TABLE mongo_dates (id Int32, d DateTime64(3, 'UTC'), s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO mongo_dates VALUES (1, '2021-03-20 11:30:05.123', '2021-03-20 11:30:05');
    CREATE USER ${user};
"

mongo() {
    ${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 "$@"
}

# Prints the name of the error of a query.
err() {
    mongo "$@" 2>&1 >/dev/null | grep -o -m1 '([A-Z_]*)' | head -1
}

echo '-- $rename of a subdocument replaces the target'
mongo --mutations_sync 2 --query 'db.mongo_rename.updateMany({}, {"$rename" : {"profile" : "user"}});'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM mongo_rename"

echo '-- an update without access to the table reveals nothing about its columns'
err --user "${user}" --query 'db.mongo_secret.updateMany({}, {"$unset" : {"profile" : ""}, "$set" : {"profile.secret" : 2}});'
err --user "${user}" --query 'db.mongo_secret.updateMany({}, {"$unset" : {"profile" : ""}});'

echo '-- the nesting of a query is bounded by max_parser_depth'
nested='{"id" : 1}'
for _ in $(seq 1 30); do nested="{\"\$and\" : [${nested}]}"; done
err --max_parser_depth 20 --query "db.mongo_secret.find(${nested}, {\"id\" : 1});"
mongo --query "db.mongo_secret.find(${nested}, {\"id\" : 1});"

echo '-- a query with leading whitespace'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&dialect=mongo&allow_experimental_mongo_dialect=1" --data-binary $'\n  db.mongo_secret.find({}, {"id" : 1})'

echo '-- the date formats are the ones of MongoDB'
mongo --query 'db.mongo_dates.aggregate([{"$project" : {"default" : {"$dateToString" : {"date" : "$d"}}, "minutes" : {"$dateToString" : {"date" : "$d", "format" : "%H:%M %% %d/%m/%Y"}}, "week" : {"$dateToString" : {"date" : "$d", "format" : "%w %u %U %V %G %j"}}, "zone" : {"$dateToString" : {"date" : "$d", "format" : "%H:%M%z", "timezone" : "Asia/Kolkata"}}}}]);'
mongo --query 'db.mongo_dates.aggregate([{"$project" : {"parsed" : {"$dateFromString" : {"dateString" : "$s", "format" : "%Y-%m-%d %H:%M:%S"}}}}]);'
err --query 'db.mongo_dates.aggregate([{"$project" : {"v" : {"$dateToString" : {"date" : "$d", "format" : "%Z"}}}}]);'
err --query 'db.mongo_dates.aggregate([{"$project" : {"v" : {"$dateFromString" : {"dateString" : "$s", "format" : "%L"}}}}]);'

echo '-- the options of $dateTrunc'
mongo --query 'db.mongo_dates.aggregate([{"$project" : {"hour" : {"$dateTrunc" : {"date" : "$d", "unit" : "hour", "binSize" : 1}}, "week" : {"$dateTrunc" : {"date" : "$d", "unit" : "week", "startOfWeek" : "Monday"}}}}]);'
err --query 'db.mongo_dates.aggregate([{"$project" : {"v" : {"$dateTrunc" : {"date" : "$d", "unit" : "hour", "binSize" : 2}}}}]);'
err --query 'db.mongo_dates.aggregate([{"$project" : {"v" : {"$dateTrunc" : {"date" : "$d", "unit" : "hour", "timezone" : "Europe/Paris"}}}}]);'
err --query 'db.mongo_dates.aggregate([{"$project" : {"v" : {"$dateTrunc" : {"date" : "$d", "unit" : "week"}}}}]);'
err --query 'db.mongo_dates.aggregate([{"$project" : {"v" : {"$dateTrunc" : {"date" : "$d", "unit" : "week", "startOfWeek" : "sunday"}}}}]);'
err --query 'db.mongo_dates.aggregate([{"$project" : {"v" : {"$dateTrunc" : {"date" : "$d", "unit" : "day", "typo" : 1}}}}]);'

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE mongo_rename;
    DROP TABLE mongo_secret;
    DROP TABLE mongo_dates;
    DROP USER ${user};
"
