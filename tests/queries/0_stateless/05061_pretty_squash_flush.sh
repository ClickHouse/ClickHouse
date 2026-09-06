#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pretty formats squash consecutive blocks in a background thread. The rendered table has to reach
# the client as soon as it is written, not stay in the output buffer until the query finishes.
# The queries below never end on their own: they print two rows from the very first block and then
# keep reading. If nothing is delivered, the output stays empty and the test fails against the
# reference.

wait_for_lines()
{
    local file=$1
    local expected=$2

    # Up to 30 seconds.
    for _ in {1..300}
    do
        if [ "$(wc -l < "$file")" -ge "$expected" ]
        then
            return
        fi
        sleep 0.1
    done
}

wait_for_content()
{
    local file=$1
    local pattern=$2

    # Up to 30 seconds.
    for _ in {1..300}
    do
        if grep -q -- "$pattern" "$file"
        then
            return
        fi
        sleep 0.1
    done
}

echo '--- unframed output'

output="${CLICKHOUSE_TMP}/05061_pretty_squash_flush.out"
: > "$output"

# A single thread makes `clickhouse-local` pull the result synchronously in the client thread. With
# more threads the sources saturate the machine, and on a loaded host the pulling thread may not get
# the first block for many seconds - which has nothing to do with the flush under test.
$CLICKHOUSE_LOCAL --max_threads=1 --query "SELECT DISTINCT number % 2 AS x FROM numbers(1e18) FORMAT PrettyCompact" > "$output" 2>/dev/null &
pid=$!

wait_for_lines "$output" 4

# The braces keep the shell's report about the terminated job out of stderr.
{ kill "$pid"; wait "$pid"; } 2>/dev/null

cat "$output"
rm -f "$output"

echo '--- framed output'

# Under `framing_output_format` the table is written into the framing payload buffer, and it reaches
# the client only when a packet boundary is taken. The boundary that `IOutputFormat::work` takes
# after `consume` is useless here - `consume` only appends to the squashed chunk - so the background
# writer has to take its own boundary once the table is rendered.

framed="${CLICKHOUSE_TMP}/05061_pretty_squash_flush_framed.out"
: > "$framed"

# The stateless test server caps `max_rows_to_read` in its default profile, which `numbers(1e18)`
# exceeds before the query even starts (`clickhouse-local` above has no such cap).
URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&framing_output_format=JSONEachPacketString&max_block_size=1&interactive_delay=3600000000&output_format_pretty_squash_consecutive_ms=50&max_execution_time=60&cancel_http_readonly_queries_on_client_close=1&max_rows_to_read=0"

${CLICKHOUSE_CURL_COMMAND} -q -sS --no-buffer --max-time 60 "$URL" \
    -d "SELECT DISTINCT number % 2 AS x FROM numbers(1e18) FORMAT PrettyCompact" > "$framed" 2>/dev/null &
pid=$!

# A `data` packet carrying the rendered table arrives while the query is still running. Other packet
# kinds (`profile_events`, `log`) may precede it, so wait for the `data` packet itself rather than
# for the first line.
wait_for_content "$framed" '"packet":"data"'

{ kill "$pid"; wait "$pid"; } 2>/dev/null

grep -o -m 1 '"packet":"data"' "$framed"
rm -f "$framed"
