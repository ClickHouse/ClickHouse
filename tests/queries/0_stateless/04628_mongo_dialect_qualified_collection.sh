#!/usr/bin/env bash
# The Mongo dialect addresses a collection as `<database>.<collection>`, so the same
# collection name in two databases is two different tables. The dialect cannot parse a
# `SET` statement, hence the setup and the teardown run in separate invocations.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FIRST="${CLICKHOUSE_DATABASE}_first"
SECOND="${CLICKHOUSE_DATABASE}_second"

${CLICKHOUSE_CLIENT} --multiquery "
    DROP DATABASE IF EXISTS ${FIRST};
    DROP DATABASE IF EXISTS ${SECOND};
    CREATE DATABASE ${FIRST};
    CREATE DATABASE ${SECOND};
    CREATE TABLE ${FIRST}.users (id Int32, name String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${SECOND}.users (id Int32, name String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${FIRST}.users VALUES (1, 'first-one'), (2, 'first-two');
    INSERT INTO ${SECOND}.users VALUES (1, 'second-one'), (2, 'second-two');
"

# `max_threads = 1` makes the read order deterministic: a `find` has no `ORDER BY`.
MONGO_CLIENT="${CLICKHOUSE_CLIENT} --dialect mongo --allow_experimental_mongo_dialect 1 --max_threads 1"

echo 'both collections'
${MONGO_CLIENT} --query "${FIRST}.users.find({});"
${MONGO_CLIENT} --query "${SECOND}.users.find({});"

echo 'a filter applies to the named database only'
${MONGO_CLIENT} --query "${FIRST}.users.find({\"id\" : 1});"
${MONGO_CLIENT} --query "${SECOND}.users.find({\"id\" : 1});"

echo 'a projection'
${MONGO_CLIENT} --query "${FIRST}.users.find({\"\$projection\" : {\"who\" : \"name\"}});"

echo 'deleteMany affects the named database only'
${MONGO_CLIENT} --query "${FIRST}.users.deleteMany({\"id\" : 1});"
${CLICKHOUSE_CLIENT} --query "SELECT id, name FROM ${FIRST}.users ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT id, name FROM ${SECOND}.users ORDER BY id"

echo 'an unknown database is an error'
${MONGO_CLIENT} --query "${FIRST}_missing.users.find({});" 2>&1 | grep -o -m1 'UNKNOWN_TABLE\|UNKNOWN_DATABASE'

${CLICKHOUSE_CLIENT} --multiquery "
    DROP DATABASE ${FIRST};
    DROP DATABASE ${SECOND};
"
