#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: the test detaches and renames databases it creates explicitly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dependency on a named collection is tracked by the UUID of the dependent table, and the names
# stored next to it deliberately go stale on `RENAME`: the UUID does not change. While the table is
# attached the UUID lookup finds it, but a detached table is only visible through its metadata file,
# which is named after the current name. Dropping the collection used to be allowed in these cases,
# and then the `ATTACH` replayed at the next server start threw `NAMED_COLLECTION_DOESNT_EXIST`.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_renamed_db"
DISK_DB="${CLICKHOUSE_DATABASE}_disk_db"

echo "--- a table detached under a new name still holds the collection ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
RENAME TABLE t TO t_renamed;
DETACH TABLE t_renamed;
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${NC}';
ATTACH TABLE t_renamed;
DROP TABLE t_renamed;
DROP NAMED COLLECTION ${NC};
"

echo "--- the same when the whole database is renamed before the detach ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.t (x UInt32) ENGINE = URL(${NC});
RENAME DATABASE ${DB} TO ${DB}_2;
DETACH TABLE ${DB}_2.t;
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${NC}';
ATTACH TABLE ${DB}_2.t;
DROP DATABASE ${DB}_2;
DROP NAMED COLLECTION ${NC};
"

echo "--- and when the renamed database is detached altogether ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.t (x UInt32) ENGINE = URL(${NC});
RENAME DATABASE ${DB} TO ${DB}_2;
DETACH DATABASE ${DB}_2;
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${NC}';
ATTACH DATABASE ${DB}_2;
DROP DATABASE ${DB}_2;
DROP NAMED COLLECTION ${NC};
"

echo "--- a detached database that keeps table metadata on its own disk still holds it ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE DATABASE ${DISK_DB} ENGINE = Atomic SETTINGS disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/');
CREATE TABLE ${DISK_DB}.t (x UInt32) ENGINE = URL(${NC});
DETACH DATABASE ${DISK_DB};
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${NC}';
ATTACH DATABASE ${DISK_DB};
DROP DATABASE ${DISK_DB};
DROP NAMED COLLECTION ${NC};
"

echo "--- a failed CREATE TABLE in the renamed database does not hold it ---"
# The stale dependency of a table that never came to exist must be recognized as stale even when the
# search for its UUID sweeps over renamed and detached databases.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.t (x UInt32) ENGINE = URL(${NC}); -- { serverError UNKNOWN_FORMAT }
RENAME DATABASE ${DB} TO ${DB}_2;
DETACH DATABASE ${DB}_2;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH DATABASE ${DB}_2;
DROP DATABASE ${DB}_2;
"
