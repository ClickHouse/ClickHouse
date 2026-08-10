#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: the test creates databases with explicit engines and detaches them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RENAME TABLE` can move a table between an `Ordinary` and an `Atomic` database. The move into
# `Atomic` assigns a fresh UUID to the table and the move out of it drops the UUID, while the
# dependency on a named collection is keyed by the UUID for `Atomic` tables and by the name for
# `Ordinary` ones. The dependency of the moved table used to keep the identity it had before the
# move, so nothing found it afterwards, and `DROP NAMED COLLECTION` was allowed even though the
# metadata of the table still referenced the collection.

NC="nc_${CLICKHOUSE_DATABASE}"
ORD="${CLICKHOUSE_DATABASE}_ordinary"
ATOM="${CLICKHOUSE_DATABASE}_atomic"

${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -m -q "
CREATE DATABASE ${ORD} ENGINE = Ordinary;
CREATE DATABASE ${ATOM} ENGINE = Atomic;
"

echo "--- a table moved from Ordinary to Atomic still holds the collection while attached ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${ORD}.t (x UInt32) ENGINE = URL(${NC});
RENAME TABLE ${ORD}.t TO ${ATOM}.t;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"

echo "--- and after it is detached ---"
${CLICKHOUSE_CLIENT} -m -q "
DETACH TABLE ${ATOM}.t;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.named_collections WHERE name = '${NC}'"

echo "--- and when its database is detached altogether ---"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH TABLE ${ATOM}.t;
DROP TABLE ${ATOM}.t;
CREATE TABLE ${ORD}.t2 (x UInt32) ENGINE = URL(${NC});
RENAME TABLE ${ORD}.t2 TO ${ATOM}.t2;
DETACH DATABASE ${ATOM};
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.named_collections WHERE name = '${NC}'"

echo "--- the collection is released once the moved table is dropped ---"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH DATABASE ${ATOM};
DROP TABLE ${ATOM}.t2;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

echo "--- the same for a table moved from Atomic to Ordinary ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${ATOM}.t3 (x UInt32) ENGINE = URL(${NC});
RENAME TABLE ${ATOM}.t3 TO ${ORD}.t3;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -m -q "
DETACH TABLE ${ORD}.t3;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.named_collections WHERE name = '${NC}'"

echo "--- and it is released when that table is dropped ---"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH TABLE ${ORD}.t3;
DROP TABLE ${ORD}.t3;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
DROP DATABASE ${ORD};
DROP DATABASE ${ATOM};
"
