#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Substrait is not built in fast test

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Of each exception only the message is printed.
$CLICKHOUSE_CLIENT --enable_substrait 1 --ignore-error -q "
    DROP TABLE IF EXISTS t_substrait_comparison;
    CREATE TABLE t_substrait_comparison
    (
        id Int64,
        d Decimal(10, 2), d_same Decimal(10, 2), d_nullable Nullable(Decimal(10, 2)), d_wide Decimal(18, 2),
        f FixedString(3), f_same FixedString(3), f_long FixedString(5),
        a Array(Int32), a_same Array(Int32), a_wide Array(Int64)
    )
    ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_substrait_comparison (id) VALUES (1);

    SELECT 'Two values of one type compare';
    SELECT JSON_QUERY(explain, '\$.extensions[*].extensionFunction.name') FROM (EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_comparison WHERE d < d_same);
    SELECT JSON_QUERY(explain, '\$.extensions[*].extensionFunction.name') FROM (EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_comparison WHERE f = f_same);
    SELECT JSON_QUERY(explain, '\$.extensions[*].extensionFunction.name') FROM (EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_comparison WHERE a = a_same);

    SELECT 'Nullable and not Nullable values of one type compare';
    SELECT JSON_QUERY(explain, '\$.extensions[*].extensionFunction.name') FROM (EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_comparison WHERE d < d_nullable);

    SELECT 'Decimal of different precision';
    EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_comparison WHERE d < d_wide;

    SELECT 'FixedString of different length';
    EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_comparison WHERE f = f_long;

    SELECT 'Array of different element type';
    EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_comparison WHERE a = a_wide;

    DROP TABLE t_substrait_comparison;
" 2>&1 | sed -E '/^Received exception/d; /^\(query:/d; s/^Code: .*DB::Exception: //; s/\. \(NOT_IMPLEMENTED\)$//'
