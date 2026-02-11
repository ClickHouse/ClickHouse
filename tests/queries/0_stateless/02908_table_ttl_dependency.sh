#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Tag no-ordinary-database: requires UUID
# Tag no-replicated-database: in replicated databases with database_replicated_always_detach_permanently, DROP TABLE
# becomes a local permanent detach bypassing the DDL queue, which breaks the dependency tracking and can leave
# orphaned table metadata that prevents server restart after stress tests

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS 02908_dependent;
    DROP TABLE IF EXISTS 02908_main;

    CREATE TABLE 02908_main (a UInt32) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE 02908_dependent (a UInt32, ts DateTime) ENGINE = MergeTree ORDER BY a TTL ts + 1 WHERE a IN (SELECT a FROM ${CLICKHOUSE_DATABASE}.02908_main);
"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE 02908_main;
" 2>&1 | grep -F -q "HAVE_DEPENDENT_OBJECTS"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE 02908_dependent;
    DROP TABLE 02908_main;
"
