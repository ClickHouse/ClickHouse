#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: SQL named collections are local to a server, so the replicas of the test database cannot resolve the collection when they execute the replicated `CREATE TABLE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `remote` table function or a `Remote` table engine persists its table function target and resolves
# it only when the local shard is read, so the named collections the target references must be held
# while the table exists, in addition to the collection of the addresses, if any.

TARGET_NC="target_nc_${CLICKHOUSE_DATABASE}"
ADDR_NC="addr_nc_${CLICKHOUSE_DATABASE}"
REMOTE_NC="remote_nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${TARGET_NC} AS url = 'http://localhost:8123', format = 'CSV', structure = 'x UInt8';
CREATE NAMED COLLECTION ${ADDR_NC} AS addresses_expr = '127.0.0.1';
CREATE NAMED COLLECTION ${REMOTE_NC} AS addresses_expr = '127.0.0.1', database = 'system', table = 'one';
"

echo "--- CREATE TABLE ... AS remote(..., url(nc)) ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE t_as (x UInt8) AS remote('127.0.0.1', url(${TARGET_NC}));
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${TARGET_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_as;
"

echo "--- ENGINE = Remote(..., url(nc)) ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE t_engine (x UInt8) ENGINE = Remote('127.0.0.1', url(${TARGET_NC}));
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${TARGET_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_engine;
"

echo "--- remote(addr_nc, database = url(target_nc)) holds both collections ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE t_both (x UInt8) AS remote(${ADDR_NC}, database = url(${TARGET_NC}));
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${TARGET_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP NAMED COLLECTION ${ADDR_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_both;
"

echo "--- a nested remote(nc) / remoteSecure(nc) target holds its collection ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE t_nested_remote (dummy UInt8) ENGINE = Remote('127.0.0.1', remote(${REMOTE_NC}));
CREATE TABLE t_nested_remote_as (dummy UInt8) AS remote('127.0.0.1', remoteSecure(${REMOTE_NC}));
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${REMOTE_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_nested_remote;
DROP NAMED COLLECTION ${REMOTE_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_nested_remote_as;
"

echo "--- the collections can be dropped once the tables are gone ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${TARGET_NC};
DROP NAMED COLLECTION ${ADDR_NC};
DROP NAMED COLLECTION ${REMOTE_NC};
SELECT count() FROM system.named_collections WHERE name IN ('${TARGET_NC}', '${ADDR_NC}', '${REMOTE_NC}');
"
