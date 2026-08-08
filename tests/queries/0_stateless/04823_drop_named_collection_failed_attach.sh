#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: the test renames databases it creates explicitly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The dependencies of a table are registered while its engine arguments are resolved, and an `ATTACH`
# can still fail after that (checking the format name, creating the storage, ...), leaving the table
# detached. The record of the detached table must survive such a failed attach: it must still block
# `DROP NAMED COLLECTION`, or the `ATTACH` replayed at the next server start would throw
# `NAMED_COLLECTION_DOESNT_EXIST` and the server would not start.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_renamed_db"

echo "--- a failed ATTACH does not unmark the detached table ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t;
ALTER NAMED COLLECTION ${NC} SET format = 'ThisFormatDoesNotExist';
ATTACH TABLE t; -- { serverError UNKNOWN_FORMAT }
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
# The first refused drop cleans up the failed attach's dependency of a table that does not exist in the
# catalog; the detached record must keep blocking the second attempt on its own.
${CLICKHOUSE_CLIENT} -m -q "
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
# Fixing the collection recovers the table, and dropping the table releases the collection.
${CLICKHOUSE_CLIENT} -m -q "
ALTER NAMED COLLECTION ${NC} SET format = 'CSV';
ATTACH TABLE t;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't';
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
"

echo "--- a rename of the re-attached table releases its detached record ---"
# After a successful `ATTACH` the record lingers until the table proves it exists again. `RENAME` frees
# the old name (only an attached table can be renamed), so the record must go with it: afterwards the
# collection is only held by the regular dependency of the attached table, and dropping the table
# releases it.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t;
ATTACH TABLE t;
RENAME TABLE t TO t_renamed;
DROP TABLE t_renamed;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

echo "--- the record of a detached table follows a rename of its database ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE ${DB}.t;
RENAME DATABASE ${DB} TO ${DB}_2;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
# The record was re-keyed to the new database name: dropping the re-attached table under it releases
# the collection.
${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${NC}';
ATTACH TABLE ${DB}_2.t;
DROP TABLE ${DB}_2.t;
DROP NAMED COLLECTION ${NC};
DROP DATABASE ${DB}_2;
"
