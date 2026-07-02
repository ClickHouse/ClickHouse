#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src_04329; DROP VIEW IF EXISTS vw_04329; DROP TABLE IF EXISTS mv_04329;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE src_04329 (x Int64) ENGINE=MergeTree ORDER BY x;"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW vw_04329 AS SELECT x FROM src_04329 WHERE x > 10;"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW mv_04329 TO vw_04329 AS SELECT x FROM src_04329;"

# Inserting into the source pushes to mv_04329, whose target vw_04329 is a View and cannot be written to.
# The error message must name both the materialized view and its target table (db name normalized for the test).
${CLICKHOUSE_CLIENT} --query "INSERT INTO src_04329 VALUES (100);" 2>&1 \
    | grep -oF "Method write is not supported by storage View: while writing to target table $CLICKHOUSE_DATABASE.vw_04329 of materialized view $CLICKHOUSE_DATABASE.mv_04329" \
    | sed "s/$CLICKHOUSE_DATABASE/{db}/g" \
    | head -n 1

# A direct INSERT into a View keeps the plain message (no materialized view context).
${CLICKHOUSE_CLIENT} --query "INSERT INTO vw_04329 VALUES (100);" 2>&1 \
    | grep -oF "Method write is not supported by storage View" \
    | head -n 1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src_04329; DROP VIEW IF EXISTS vw_04329; DROP TABLE IF EXISTS mv_04329;"
