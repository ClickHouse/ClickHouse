#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: the test detaches a database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In a database with `lazy_load_tables = 1` a table is attached as a `StorageTableProxy` and its real
# storage is built only on the first access, so the engine arguments are not resolved at load time and
# the dependency on the named collection they name used to stay unregistered. `DROP NAMED COLLECTION`
# was then allowed even though the metadata of the table still referenced the collection, and a
# `DETACH` of such a table had no dependency to move to the list of the detached ones either.

NC="nc_${CLICKHOUSE_DATABASE}"
LZ="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} -m -q "
CREATE DATABASE ${LZ} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${LZ}.t (x UInt32) ENGINE = URL(${NC});
"

echo "--- a never-accessed lazy proxy holds the collection ---"
# `DETACH DATABASE`/`ATTACH DATABASE` brings the table back as a proxy, as a restart would.
${CLICKHOUSE_CLIENT} -m -q "
DETACH DATABASE ${LZ};
ATTACH DATABASE ${LZ};
SELECT engine FROM system.tables WHERE database = '${LZ}' AND name = 't';
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.named_collections WHERE name = '${NC}'"

echo "--- and it keeps holding it after the table is detached ---"
${CLICKHOUSE_CLIENT} -m -q "
DETACH TABLE ${LZ}.t;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.named_collections WHERE name = '${NC}'"

echo "--- the table is still there after it is attached back ---"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH TABLE ${LZ}.t;
SELECT count() FROM system.tables WHERE database = '${LZ}' AND name = 't';
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"

echo "--- and the collection is released when the table is dropped ---"
${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE ${LZ}.t;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
DROP DATABASE ${LZ};
"
