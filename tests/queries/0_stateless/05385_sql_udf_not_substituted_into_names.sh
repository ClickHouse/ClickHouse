#!/usr/bin/env bash
# A SQL UDF replaces function calls, but not the name of a statistics type, codec, index type, engine, table function
# or aggregate function.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# SQL UDFs are server-global, so the names carry the test database.
F="udf_f_05385_${CLICKHOUSE_DATABASE}"
G="udf_g_05385_${CLICKHOUSE_DATABASE}"
T="udf_t_05385_${CLICKHOUSE_DATABASE}"
H="udf_h_05385_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
CREATE FUNCTION $F AS (x) -> 2;
CREATE FUNCTION $G AS (x) -> Memory();

CREATE TABLE t_stat (c Int64 STATISTICS($F(2), tdigest)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE t_codec (c Int64 CODEC($F(2))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_CODEC }
CREATE TABLE t_index (c Int64, INDEX i c TYPE $F(2) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE t_proj (c Int64, PROJECTION p INDEX c TYPE $F(2)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE t_engine (c Int64) ENGINE = $G(1); -- { serverError UNKNOWN_STORAGE }
CREATE TABLE t_tf AS $F(3); -- { serverError UNKNOWN_FUNCTION }
CREATE TABLE t_agg (c AggregateFunction($F(2), UInt64)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }
CREATE TABLE t_sagg (c SimpleAggregateFunction($F(2), UInt64)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

CREATE TABLE t (c Int64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t ADD STATISTICS c TYPE $F(2); -- { serverError INCORRECT_QUERY }
ALTER TABLE t MODIFY COLUMN c Int64 STATISTICS($F(2)); -- { serverError INCORRECT_QUERY }

-- Expressions, lambda bodies and the arguments of a name are still substituted.
CREATE TABLE t_ok
(
    c Int64 DEFAULT $F(1) STATISTICS(tdigest),
    l Array(UInt8) DEFAULT arrayMap(x -> $F(x), [1]),
    a AggregateFunction(uniqUpTo($F(1)), UInt8),
    INDEX i c TYPE set($F(1)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
SELECT name, default_expression, type FROM system.columns WHERE database = currentDatabase() AND table = 't_ok' ORDER BY position;
SELECT type_full FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_ok';

-- A name inside a UDF body is not substituted either.
CREATE VIEW $T AS SELECT number FROM numbers(10) WHERE number < {p:UInt64};
CREATE FUNCTION $T AS (a) -> a;
CREATE FUNCTION $H AS (x) -> (SELECT count() FROM $T(p = x));
CREATE VIEW v_body AS SELECT $H(3) AS n;
SELECT n FROM v_body;

-- \`lambda\` cannot carry the test database in its name; with the fix this UDF changes nothing for concurrent queries.
CREATE OR REPLACE FUNCTION lambda AS (a, b) -> b;
CREATE TABLE t_lambda
(
    s UInt8 DEFAULT 5 < SOME([6]),
    w Array(UInt8) DEFAULT arrayMap(lambda(tuple(x), x + 1), [1]),
    a Array(UInt8) DEFAULT arrayMap(x -> x + 1, [1])
)
ENGINE = MergeTree ORDER BY tuple();
SELECT name, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_lambda' ORDER BY position;

DROP FUNCTION $F;
DROP FUNCTION $G;
DROP FUNCTION $H;
DROP FUNCTION $T;
DROP FUNCTION IF EXISTS lambda;
"
