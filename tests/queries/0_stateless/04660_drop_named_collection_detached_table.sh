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

echo "--- a stale dependency of a failed CREATE TABLE does not hold it ---"
# A dependency is registered while the engine arguments are resolved, which happens before the table is
# created, so a `CREATE TABLE` that fails after that point leaves the dependency of a table that never
# came to exist behind. It must not block the drop, not even when the name is taken by another table
# afterwards.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE TABLE t3 (x UInt32) ENGINE = URL(${NC}); -- { serverError UNKNOWN_FORMAT }
CREATE TABLE t3 (x UInt32) ENGINE = Memory;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
DROP TABLE t3;
"

echo "--- in an Ordinary database, the name is all there is to identify the table by ---"
# In an Ordinary database the dependency carries no UUID, so when the name is taken by another table
# afterwards, the stale dependency cannot be told apart from a dependency of that table: the drop is
# refused until the name is freed. The check is deliberately imprecise in this direction.
${CLICKHOUSE_CLIENT} --send_logs_level=error -m -q "
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${ORDINARY_DB} ENGINE = Ordinary;
"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE TABLE ${ORDINARY_DB}.t (x UInt32) ENGINE = URL(${NC}); -- { serverError UNKNOWN_FORMAT }
CREATE TABLE ${ORDINARY_DB}.t (x UInt32) ENGINE = Memory;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE ${ORDINARY_DB}.t;
DROP DATABASE ${ORDINARY_DB};
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

echo "--- and in a detached database, where the table has no metadata to be attached from ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE DATABASE ${DETACHED_DB};
CREATE TABLE ${DETACHED_DB}.t (x UInt32) ENGINE = URL(${NC}); -- { serverError UNKNOWN_FORMAT }
DETACH DATABASE ${DETACHED_DB};
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH DATABASE ${DETACHED_DB};
DROP DATABASE ${DETACHED_DB};
"

echo "--- a dependency on another collection is not lost when a stale one is cleaned up ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE NAMED COLLECTION ${NC}_2 AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t4 (x UInt32) ENGINE = URL(${NC}); -- { serverError UNKNOWN_FORMAT }
CREATE TABLE t4 (x UInt32) ENGINE = URL(${NC}_2);
DROP NAMED COLLECTION ${NC};
DROP NAMED COLLECTION ${NC}_2; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t4;
DROP NAMED COLLECTION ${NC}_2;
"

echo "--- a permanently detached table does not hold it: it is not loaded at startup ---"
# A permanently detached table cannot break the server start, so it does not block the drop - neither
# right away nor after a restart (which would empty the in-memory list of detached dependencies anyway).
# A later `ATTACH` fails cleanly with a missing collection; recreating the collection recovers the table.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t PERMANENTLY;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH TABLE t; -- { serverError NAMED_COLLECTION_DOESNT_EXIST }
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
ATTACH TABLE t;
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
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
