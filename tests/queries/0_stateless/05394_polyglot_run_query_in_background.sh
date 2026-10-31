#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./04666_run_query_in_background.lib
. "$CUR_DIR"/04666_run_query_in_background.lib

# A background query re-parses the original text on the background thread. It must be parsed under
# the same settings as the foreground parse, not under the ones installed by the query's own `SETTINGS`
# clause: a query that switches off the polyglot gate or the dialect for its own execution is still
# transpiled the same way.

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n"

url="${CLICKHOUSE_URL}&async_insert=0&run_query_in_background=1&allow_experimental_polyglot_dialect=1&dialect=polyglot&polyglot_dialect=clickhouse"

gate_id="gate_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CURL} -sS "${url}&query_id=${gate_id}" -d "INSERT INTO t SELECT 41 + 1 SETTINGS allow_experimental_polyglot_dialect = 0"
dialect_id="dialect_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CURL} -sS "${url}&query_id=${dialect_id}" -d "INSERT INTO t SELECT 43 SETTINGS dialect = 'clickhouse'"

wait_for_query_log "count() = 2 FROM system.query_log WHERE current_database = currentDatabase() AND query_id IN ('$gate_id', '$dialect_id') AND type != 'QueryStart'"
$CLICKHOUSE_CLIENT -q "
    SELECT query_id = '$gate_id' ? 'gate' : 'dialect', type, exception_code FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id IN ('$gate_id', '$dialect_id') AND type != 'QueryStart' ORDER BY 1;
    SELECT n FROM t ORDER BY n;
    DROP TABLE t"
