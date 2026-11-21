#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: SQL named collections are local to a server, so the replicas of the test database cannot resolve the collection when they execute the replicated `CREATE TABLE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table function target of a `Remote` table is resolved only when the local shard is read, so the
# table attaches even while a collection its target references is missing (after a drop with
# `check_named_collection_dependencies = 0`). The collection must be held as soon as it is created
# again: whether it references a collection is decided from the stored definition, not from whether a
# collection with that name exists when the table is attached.

NC="nested_nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV', structure = 'x UInt8';
CREATE TABLE t_engine (x UInt8) ENGINE = Remote('127.0.0.1', url(${NC}));
CREATE TABLE t_as (x UInt8) AS remote('127.0.0.1', url(${NC}));
SET check_named_collection_dependencies = 0;
DROP NAMED COLLECTION ${NC};
DETACH TABLE t_engine;
DETACH TABLE t_as;
ATTACH TABLE t_engine;
ATTACH TABLE t_as;
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV', structure = 'x UInt8';
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_engine;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_as;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
