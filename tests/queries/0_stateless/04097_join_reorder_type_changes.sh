#!/usr/bin/env bash
# Regression test: join reorder with type-changing joins (e.g. LEFT JOIN + join_use_nulls)
# could cause "Cannot fold actions for projection" when the optimizer separates a relation
# from the join that causes its type change.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t1 (id Int32, a Int32, b Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t2 (id Int32, c Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t3 (id Int32, a Int32, c Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t4 (id Int32, a Int32) ENGINE = MergeTree ORDER BY id;

    INSERT INTO ${CLICKHOUSE_DATABASE}.t1 VALUES (1, 1, 1);
    INSERT INTO ${CLICKHOUSE_DATABASE}.t2 VALUES (1, 1);
    INSERT INTO ${CLICKHOUSE_DATABASE}.t3 VALUES (1, 1, 1);
    INSERT INTO ${CLICKHOUSE_DATABASE}.t4 VALUES (1, 1);

    SELECT t2.id
    FROM ${CLICKHOUSE_DATABASE}.t2
        LEFT JOIN ${CLICKHOUSE_DATABASE}.t3 ON t2.c = t3.c
        LEFT JOIN ${CLICKHOUSE_DATABASE}.t1 ON t3.a = t1.a
        INNER JOIN ${CLICKHOUSE_DATABASE}.t4 ON t1.id IS NOT NULL AND t1.a = t4.a
    ORDER BY ALL
    SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10;

    DROP TABLE ${CLICKHOUSE_DATABASE}.t1;
    DROP TABLE ${CLICKHOUSE_DATABASE}.t2;
    DROP TABLE ${CLICKHOUSE_DATABASE}.t3;
    DROP TABLE ${CLICKHOUSE_DATABASE}.t4;
"
