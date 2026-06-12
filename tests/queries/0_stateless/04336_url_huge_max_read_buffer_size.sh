#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279
# The url table function builds its HTTP read buffer from max_read_buffer_size via
# ReadWriteBufferFromHTTP::withBufSize. Setting it to a very large value used to throw
# std::length_error while allocating the buffer. The configured value is only an upper bound;
# the allocation must be capped and the read must keep working.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --max_read_buffer_size 18446744073709551615 --query "
SELECT sum(x) FROM url(
    \$\$http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(100)\$\$,
    TSV,
    'x UInt64')"
