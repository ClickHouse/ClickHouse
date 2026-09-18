#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# no-ordinary-database: a full-definition ATTACH needs the UUID clause and refreshable views need Atomic.
# no-replicated-database: explicit UUIDs are not allowed and POPULATE is rejected in Replicated databases by default.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Materialized-view query paths other than the insert push run with enable_global_with_statement pinned;
# a full-definition ATTACH is fresh user input and is validated like CREATE.
# https://github.com/ClickHouse/ClickHouse/issues/113711
# `same` compares rand64() taken from two references of one CTE: 1 = one materialization, 0 = inlined twice.

UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} -nm -q "
DROP TABLE IF EXISTS src_113711_sh, r_113711_sh, dst_attach_113711, dst_populate_113711, dst_refresh_113711;
DROP TABLE IF EXISTS mv_attach_113711, mv_populate_113711, mv_refresh_113711;
CREATE TABLE src_113711_sh (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO src_113711_sh VALUES (1), (2), (3);
CREATE TABLE r_113711_sh (id UInt32, x UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO r_113711_sh SELECT number, number FROM numbers(7);
CREATE TABLE dst_attach_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_populate_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_refresh_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
"

DEFINITION="WITH r_113711_sh AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711_sh) SELECT a.id AS id, a.x = b.x AS same FROM (SELECT id, x FROM r_113711_sh) AS a INNER JOIN r_113711_sh AS b ON a.id = b.id"

echo "-- a full-definition ATTACH is fresh input: fixing enable_global_with_statement is rejected"
${CLICKHOUSE_CLIENT} --send_logs_level fatal -q "ATTACH MATERIALIZED VIEW mv_attach_113711 UUID '$UUID' TO dst_attach_113711 (id UInt32, same UInt8) AS $DEFINITION SETTINGS enable_global_with_statement = 0" 2>&1 | grep -oF 'NOT_IMPLEMENTED' | head -n 1
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_attach_113711'"

echo "-- without the clause it is accepted and keeps the reference, so the nested reference reads the CTE, not the same-named table"
${CLICKHOUSE_CLIENT} --send_logs_level fatal -q "ATTACH MATERIALIZED VIEW mv_attach_113711 UUID '$UUID' TO dst_attach_113711 (id UInt32, same UInt8) AS $DEFINITION"
${CLICKHOUSE_CLIENT} --enable_materialized_cte 1 -q "INSERT INTO src_113711_sh SETTINGS enable_global_with_statement = 0 VALUES (81), (82)"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM dst_attach_113711 ORDER BY id"

echo "-- POPULATE runs with the setting pinned"
${CLICKHOUSE_CLIENT} --enable_global_with_statement 0 --enable_materialized_cte 1 -q "CREATE MATERIALIZED VIEW mv_populate_113711 TO dst_populate_113711 POPULATE AS WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711_sh) SELECT a.id AS id, a.x = b.x AS same FROM (SELECT id, x FROM r) AS a INNER JOIN r AS b ON a.id = b.id"
${CLICKHOUSE_CLIENT} -q "SELECT count() = (SELECT count() FROM src_113711_sh), min(same), max(same) FROM dst_populate_113711"

echo "-- a refreshable view reads the CTE when its definition enables enable_materialized_cte"
${CLICKHOUSE_CLIENT} --enable_global_with_statement 0 --enable_materialized_cte 1 -q "CREATE MATERIALIZED VIEW mv_refresh_113711 REFRESH EVERY 1 YEAR TO dst_refresh_113711 AS WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711_sh) SELECT a.id AS id, a.x = b.x AS same FROM (SELECT id, x FROM r) AS a INNER JOIN r AS b ON a.id = b.id SETTINGS enable_materialized_cte = 1"
${CLICKHOUSE_CLIENT} -q "SYSTEM REFRESH VIEW mv_refresh_113711"
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT VIEW mv_refresh_113711"
${CLICKHOUSE_CLIENT} -q "SELECT count() = (SELECT count() FROM src_113711_sh), min(same), max(same) FROM dst_refresh_113711"

${CLICKHOUSE_CLIENT} -nm -q "
DROP TABLE mv_refresh_113711, mv_populate_113711, mv_attach_113711;
DROP TABLE dst_refresh_113711, dst_populate_113711, dst_attach_113711, r_113711_sh, src_113711_sh;
"
