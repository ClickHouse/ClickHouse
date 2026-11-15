#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Framed `progress` packets must be throttled by `interactive_delay`. A pending progress update is
# also flushed at packet boundaries (when the concurrent progress write lost the lock), but that
# flush must honor the same throttle - otherwise a stream of many small blocks would emit a
# `progress` packet at every `data` boundary. With `interactive_delay` of one hour, the only
# `progress` packet of the stream is the final one (written by the framing on finalization after
# draining the trailing packets), no matter how many blocks the query produces.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"

echo '--- one hour interactive_delay: exactly one (final) progress packet for a many-block stream'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&max_block_size=10&max_threads=1&interactive_delay=3600000000" \
    -d "SELECT number FROM numbers(1000) FORMAT JSONEachRow" \
    | grep -c '"packet":"progress"'

echo '--- the final progress packet is the last packet of the stream'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&max_block_size=10&max_threads=1&interactive_delay=3600000000" \
    -d "SELECT number FROM numbers(1000) FORMAT JSONEachRow" \
    | tail -n 1 | grep -o '"packet":"progress"'
