#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/58014
#
# `clickhouse-benchmark` used to crash with
#   std::logic_error: Distribution number for Student's T-Test must be either 0 or 1
# when three or more endpoints were passed via -h/--host without --roundrobin: the
# Student's T-test comparison mode only supports two distributions, but a sample was
# added for every endpoint. It must not crash anymore; the comparison report is skipped
# unless exactly two endpoints are benchmarked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Three endpoints, all pointing at the test server (no --roundrobin). Each query picks one
# of three connections randomly; before the fix, picking the third connection passed
# distribution index 2 to StudentTTest::add() and crashed. With --iterations 100 the third
# connection is hit with overwhelming probability. $CLICKHOUSE_BENCHMARK already injects one
# --port, so three --host flags plus two extra --port flags yield exactly three identical
# endpoints.
$CLICKHOUSE_BENCHMARK \
    --host "$CLICKHOUSE_HOST" --host "$CLICKHOUSE_HOST" --host "$CLICKHOUSE_HOST" \
    --port "$CLICKHOUSE_PORT_TCP" --port "$CLICKHOUSE_PORT_TCP" \
    --concurrency 1 --iterations 100 --query 'SELECT 1' >/dev/null 2>&1 \
    && echo OK || echo FAIL
