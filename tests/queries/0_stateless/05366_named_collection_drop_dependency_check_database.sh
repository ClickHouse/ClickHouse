#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the S3 and PostgreSQL database engines are not built in the fast test.

# `DROP NAMED COLLECTION` refuses to drop a collection that a database uses, as it does for a table:
# a database whose collection is dropped cannot be loaded at the next start, and the server exits.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A failed step stops the test, and the cleanup runs on any exit: a database left with its collection dropped
# (by a regression of the check) cannot be loaded at the next start, and the server exits.
set -e

DB_PG="${CLICKHOUSE_DATABASE}_pg"
DB_S3="${CLICKHOUSE_DATABASE}_s3"
DB_REMOTE="${CLICKHOUSE_DATABASE}_remote"
# A table using a collection lives in a Memory database, not in the test database: in the replicated database
# flavor the replicas would replay its CREATE without the collection, which is local to this server.
DB_TABLES="${CLICKHOUSE_DATABASE}_tables"
NC_PG="nc_pg_${CLICKHOUSE_DATABASE}"
NC_S3="nc_s3_${CLICKHOUSE_DATABASE}"
NC_REMOTE="nc_remote_${CLICKHOUSE_DATABASE}"

# The databases go before the collections, otherwise a database left by an interrupted run outlives its collection.
cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS ${DB_TABLES};
    DROP DATABASE IF EXISTS ${DB_PG};
    DROP DATABASE IF EXISTS ${DB_S3};
    DROP DATABASE IF EXISTS ${DB_REMOTE};
    DROP NAMED COLLECTION IF EXISTS ${NC_PG};
    DROP NAMED COLLECTION IF EXISTS ${NC_S3};
    DROP NAMED COLLECTION IF EXISTS ${NC_REMOTE};
    "
}

cleanup
trap cleanup EXIT

# ast_fuzzer_runs = 0: the fuzzer could clone a CREATE under another name that references a collection this test drops.
$CLICKHOUSE_CLIENT -m -q "
SET ast_fuzzer_runs = 0;
CREATE NAMED COLLECTION ${NC_PG} AS host = '127.0.0.1', port = 5432, user = 'u', password = 'p', database = 'db';
CREATE NAMED COLLECTION ${NC_S3} AS url = 'http://localhost:11111/test/';
CREATE NAMED COLLECTION ${NC_REMOTE} AS host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = 'default', password = '', database = 'default';
CREATE DATABASE ${DB_PG} ENGINE = PostgreSQL(${NC_PG});
CREATE DATABASE ${DB_S3} ENGINE = S3(${NC_S3});
CREATE DATABASE ${DB_REMOTE} ENGINE = Remote(${NC_REMOTE});
"

drop_collection()
{
    # Prints the error, or that the drop succeeded, with the name of the test database replaced.
    local output
    output=$($CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION $1" 2>&1 | grep -o -m1 'Named collection .*(NAMED_COLLECTION_IS_USED)' || echo "$1 dropped")
    echo "${output//${CLICKHOUSE_DATABASE}/test_db}"
}

echo "-- Each database blocks the drop of its collection"
drop_collection "${NC_PG}"
drop_collection "${NC_S3}"
drop_collection "${NC_REMOTE}"

echo "-- A table and a database are both reported"
# ast_fuzzer_runs = 0: the fuzzer would clone the table under a `__fuzz_N` name that also uses the collection.
# The table goes with DROP DATABASE: a DROP TABLE could be fuzzed into UNDROP, or replaced with TRUNCATE.
$CLICKHOUSE_CLIENT -m -q "
SET ast_fuzzer_runs = 0;
CREATE DATABASE ${DB_TABLES} ENGINE = Memory;
CREATE TABLE ${DB_TABLES}.pg_table (a Int32) ENGINE = PostgreSQL(${NC_PG}, table = 't');
"
drop_collection "${NC_PG}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB_TABLES}"

echo "-- The dependency is registered again when the database is attached"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${DB_PG}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${DB_PG}"
drop_collection "${NC_PG}"

echo "-- A dropped database does not block the drop"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB_PG}"
drop_collection "${NC_PG}"

echo "-- A database created after a failed creation under the same name does not inherit its dependency"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${NC_PG} AS host = '127.0.0.1', port = 5432, user = 'u', password = 'p', database = 'db'"
# The engine registers the dependency before it rejects the zero pool size, so the failed CREATE leaves a stale one.
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_PG} ENGINE = PostgreSQL(${NC_PG}) SETTINGS postgresql_connection_pool_size = 0" 2>&1 | grep -o -m1 'BAD_ARGUMENTS' || true
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_PG} ENGINE = Memory"
drop_collection "${NC_PG}"

