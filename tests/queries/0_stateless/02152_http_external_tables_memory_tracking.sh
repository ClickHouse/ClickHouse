#!/usr/bin/env bash
# Tags: no-tsan, no-cpu-aarch64
# TSan does not supports tracing.
# trace_log doesn't work on aarch64

# Regression for proper release of Context,
# via tracking memory of external tables.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

tmp_file=$(mktemp "$CURDIR/clickhouse.XXXXXX.csv")
trap 'rm $tmp_file' EXIT

$CLICKHOUSE_CLIENT -q "SELECT toString(number) FROM numbers(1e6) FORMAT TSV" > "$tmp_file"

function run_and_check()
{
    local query_id
    query_id="$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @- <<<'SELECT generateUUIDv4()')"

    echo "Checking $*"

    # Run query with external table (implicit StorageMemory user)
    $CLICKHOUSE_CURL -sS -F "s=@$tmp_file;" "$CLICKHOUSE_URL&s_structure=key+Int&query=SELECT+count()+FROM+s&memory_profiler_sample_probability=1&query_id=$query_id&$*" -o /dev/null

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @- <<<'SYSTEM FLUSH LOGS'

    # Check that temporary table had been destroyed.
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&allow_introspection_functions=1" --data-binary @- <<<"
    WITH arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS sym
    SELECT count()>0 FROM system.trace_log
    WHERE
        sym LIKE '%DB::StorageMemory::drop%\n%TemporaryTableHolder::~TemporaryTableHolder%' AND
        query_id = '$query_id'
    "
}

for input_format_parallel_parsing in false true; do
    query_args_variants=(
        ""
        "cancel_http_readonly_queries_on_client_close=1&readonly=1"
        "send_progress_in_http_headers=true"
        # nested progress callback
        "cancel_http_readonly_queries_on_client_close=1&readonly=1&send_progress_in_http_headers=true"
    )
    for query_args in "${query_args_variants[@]}"; do
        run_and_check "input_format_parallel_parsing=$input_format_parallel_parsing&$query_args"
    done
done
