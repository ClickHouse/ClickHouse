#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A regexp that fails to compile must not echo the whole (possibly multi-megabyte)
# pattern back: both the pattern and re2's error text are bounded in the message.
# Here a 100000-byte invalid pattern must yield a small error message.
len=$(${CLICKHOUSE_CLIENT} --query "SELECT match('', repeat('(', 100000))" 2>&1 | wc -c)
[ "$len" -lt 2048 ] && echo "bounded" || echo "too long: $len"
