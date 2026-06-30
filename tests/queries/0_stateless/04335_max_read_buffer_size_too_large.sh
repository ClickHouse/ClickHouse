#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279
# The read buffer for the file table function is allocated eagerly at max_read_buffer_size.
# Setting it to a very large value used to throw std::length_error during that allocation. The
# configured value is only an upper bound; the allocation is capped and the read must keep working.
# A unique file name keeps the test isolated from concurrent runs (e.g. in the flaky check).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

file="${CLICKHOUSE_DATABASE}_04335.tsv"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('$file', 'TSV') SELECT number FROM numbers(100)
    SETTINGS engine_file_truncate_on_insert = 1"

${CLICKHOUSE_CLIENT} --max_read_buffer_size 18446744073709551615 --query "
    SELECT count() FROM file('$file', 'TSV', 'x UInt64')"
