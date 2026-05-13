#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `translateUTF8` should report code point counts in the error message,
# not byte sizes, since the comparison itself is on code point counts
# 'αβ' = 2 code points (4 bytes), 'abc' = 3 code points (3 bytes)
${CLICKHOUSE_CLIENT} -q "SELECT translateUTF8('x', 'αβ', 'abc')" 2>&1 \
    | grep -oE "Size of the second argument: [0-9]+, size of the third argument: [0-9]+"

# `translate` (byte-wise) keeps reporting byte sizes, which equal code point counts for ASCII
${CLICKHOUSE_CLIENT} -q "SELECT translate('x', 'ab', 'abc')" 2>&1 \
    | grep -oE "Size of the second argument: [0-9]+, size of the third argument: [0-9]+"
