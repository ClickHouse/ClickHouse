#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

[ ! -z "$CLICKHOUSE_CLIENT_REDEFINED" ] && CLICKHOUSE_CLIENT=$CLICKHOUSE_CLIENT_REDEFINED

##################
# checking that both queries have the same `EXPLAIN SYNTAX` output
# todo: `EXPLAIN SYTNAX` can be replaced by `EXPLAIN AST rewrite=1` when available
##################
QUERY_ORDER_BY="SELECT number AS a, number % 2 AS b FROM numbers(10) ORDER BY a DESC NULLS FIRST WITH FILL FROM 2 TO 1 STEP -1, b DESC NULLS FIRST WITH FILL FROM 2 TO 1 STEP -1"
QUERY_ORDER_BY_TUPLE="SELECT number AS a, number % 2 AS b FROM numbers(10) ORDER BY (a, b) DESC NULLS FIRST WITH FILL FROM 2 TO 1 STEP -1"

EXPLAIN="EXPLAIN SYNTAX"
OUTPUT_EXPLAIN_ORDER_BY=$($CLICKHOUSE_CLIENT -q "$EXPLAIN $QUERY_ORDER_BY")
OUTPUT_EXPLAIN_ORDER_BY_TUPLE=$($CLICKHOUSE_CLIENT -q "$EXPLAIN $QUERY_ORDER_BY_TUPLE")

$CLICKHOUSE_CLIENT -q "drop table if exists order_by_syntax"
$CLICKHOUSE_CLIENT -q "create table order_by_syntax (explain String) engine=Memory"
$CLICKHOUSE_CLIENT -q "insert into order_by_syntax values('$OUTPUT_EXPLAIN_ORDER_BY')"
$CLICKHOUSE_CLIENT -q "insert into order_by_syntax values('$OUTPUT_EXPLAIN_ORDER_BY_TUPLE')"
$CLICKHOUSE_CLIENT -q "select count(distinct explain) from order_by_syntax"
$CLICKHOUSE_CLIENT -q "drop table if exists order_by_syntax"
