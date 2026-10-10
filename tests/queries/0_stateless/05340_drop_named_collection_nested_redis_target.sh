#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: SQL named collections are local to a server, so the replicas of the test database cannot resolve the collection when they execute the replicated `CREATE TABLE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `redis` table function never accepts a named collection (its first argument is always
# `host:port`, and a bare identifier is evaluated as an address), even though the `Redis` table engine
# does. A nested `redis` target of a `Remote` table must not hold a collection with the same name.

NC="redis_nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS host = 'localhost', port = 6379;
CREATE TABLE t_redis (k String) ENGINE = Remote('127.0.0.1', redis(${NC}, k, 'k String'));
CREATE TABLE t_redis_as (k String) AS remote('127.0.0.1', redis(${NC}, k, 'k String'));
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
DROP TABLE t_redis;
DROP TABLE t_redis_as;
"
