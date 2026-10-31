#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FORMAT Hash` applied by `clickhouse client` (TCP) is rendered on the client
# side, so the query must go over HTTP for the server to run the
# `HashOutputFormat` processor. The failpoint cancels the query in place, the
# same way `KILL QUERY` does, and the per-row cancellation check in
# `HashOutputFormat::consume` throws `QUERY_WAS_CANCELLED`.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT hash_output_format_cancel_mid_loop"

# `max_block_size` equal to the row count clamps `numbers` to one stream, so the whole
# result arrives as one chunk and only the per-row cancellation check can leave the row
# loop early. Without it the whole chunk would be hashed before the cancellation becomes
# visible, and the statement would complete instead of failing.
URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"
OUTPUT=$(curl -sS -X POST "$URL" --data "SELECT number, toString(number) FROM numbers(10) FORMAT Hash SETTINGS max_block_size = 10")

echo "$OUTPUT" | grep -q "QUERY_WAS_CANCELLED"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT hash_output_format_cancel_mid_loop"
