#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The LineAsString format reads every line until the newline character (\n).
# The DOS or MacOS Classic line breaks \r\n or \n\r don't have special support.

# While the behavior described above can be changed in future to add the support for DOS or MacOS Classic,
# the most important is that it should be exactly the same (consistent)
# regardless whether the parallel parsing enabled or not,
# and this test checks that.

for _ in {1..10000}; do echo -ne 'Hello\n\r'; done | $CLICKHOUSE_LOCAL --min_chunk_bytes_for_parallel_parsing 1 --input_format_parallel_parsing 0 --query "SELECT hex(*), count() FROM table GROUP BY ALL ORDER BY 2 DESC, 1" --input-format LineAsString
echo '---'
for _ in {1..10000}; do echo -ne 'Hello\n\r'; done | $CLICKHOUSE_LOCAL --min_chunk_bytes_for_parallel_parsing 1 --input_format_parallel_parsing 1 --query "SELECT hex(*), count() FROM table GROUP BY ALL ORDER BY 2 DESC, 1" --input-format LineAsString
