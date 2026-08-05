#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar needs it.
# no-replicated-database: the experimental `TimeSeries` table engine does not round-trip through `DatabaseReplicated`.

# A PromQL query with a long tail of unrecognized characters (e.g. a `FixedString` padded with NUL
# bytes) must be rejected quickly. The lexer used to recover from each bad byte one at a time and
# report an error whose position was computed by scanning the query from its start, which made the
# parse quadratic in the query length; the input below took minutes and was uncancellable, because
# parsing happens during query analysis. It now takes well under a second.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --query "CREATE TABLE ts ENGINE = TimeSeries"

# The error message quotes the query, so it carries the NUL padding - strip it, `grep` treats input
# with NUL bytes as binary and matches nothing.
timeout 60 $CLICKHOUSE_CLIENT --query "SELECT count() FROM prometheusQuery(ts, toFixedString('rate(up[2d])', 262144), 1000)" 2>&1 |
    tr -d '\0' | grep -o -m1 'CANNOT_PARSE_PROMQL_QUERY' || echo 'FAILED: the parse did not finish in time'

$CLICKHOUSE_CLIENT --query "DROP TABLE ts"
