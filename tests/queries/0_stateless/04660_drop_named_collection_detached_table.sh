#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: the test creates a database with the `Ordinary` engine.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table detached with a plain `DETACH TABLE` keeps its metadata file, so it is attached again on the
# next server start. Dropping a named collection it references used to be allowed (the table is not in
# `DatabaseCatalog`, so the dependency was considered stale), and then the server did not start at all:
# the replayed `ATTACH` threw `NAMED_COLLECTION_DOESNT_EXIST` while loading the metadata.

NC="nc_${CLICKHOUSE_DATABASE}"
ORDINARY_DB="${CLICKHOUSE_DATABASE}_ordinary"
DETACHED_DB="${CLICKHOUSE_DATABASE}_detached"

echo "--- a temporarily detached table still holds the named collection ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
# The collection is still there, so the table attaches back and the server would start.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.named_collections WHERE name = '${NC}'"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH TABLE t;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't';
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
"

echo "--- the same in an Ordinary database, where dependencies are tracked by name ---"
${CLICKHOUSE_CLIENT} --send_logs_level=error -m -q "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${ORDINARY_DB} ENGINE = Ordinary;
"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${ORDINARY_DB}.t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE ${ORDINARY_DB}.t;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH TABLE ${ORDINARY_DB}.t;
DROP TABLE ${ORDINARY_DB}.t;
DROP DATABASE ${ORDINARY_DB};
DROP NAMED COLLECTION ${NC};
"

echo "--- a table in a detached database still holds it, too ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE DATABASE ${DETACHED_DB};
CREATE TABLE ${DETACHED_DB}.t (x UInt32) ENGINE = URL(${NC});
DETACH DATABASE ${DETACHED_DB};
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH DATABASE ${DETACHED_DB};
DROP DATABASE ${DETACHED_DB};
DROP NAMED COLLECTION ${NC};
"

echo "--- a permanently detached table does not hold it: the metadata is not loaded at startup ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t PERMANENTLY;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

echo "--- check_named_collection_dependencies = 0 still allows dropping it ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t2 (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t2;
SET check_named_collection_dependencies = 0;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
# Do not leave a table whose named collection is missing behind: it would break the next server start.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
ATTACH TABLE t2;
DROP TABLE t2;
DROP NAMED COLLECTION ${NC};
"
