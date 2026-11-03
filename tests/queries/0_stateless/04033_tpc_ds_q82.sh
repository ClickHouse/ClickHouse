#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database, no-flaky-check
# no-fasttest: TPC-DS tables use web disk (S3) which is not available in fasttest.
# no-random-settings: random session_timezone, query_plan_join_swap_table, etc. change query results.
# no-replicated-database: the `tpcds` database is not created in DatabaseReplicated mode.
# no-flaky-check: TPC-DS queries are too expensive for thread fuzzer.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./04033_tpc_ds.lib
. "$CURDIR"/04033_tpc_ds.lib

# These tables carry no statistics, so `dpsub` ranks join orders on unknown cardinalities and puts
# `inventory` next to `store_sales` innermost -- a pair tied together only through `item` -- which
# builds billions of rows and runs out of memory. Pin greedy until we can estimate these properly.
{ echo "USE tpcds;"; cat "$CURDIR/../../benchmarks/tpc-ds/queries/query_82.sql"; } | $CLICKHOUSE_CLIENT "${SETTINGS[@]}" --query_plan_optimize_join_order_algorithm greedy
