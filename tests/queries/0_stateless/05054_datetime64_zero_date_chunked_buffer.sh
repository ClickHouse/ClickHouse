#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.tsv"

printf '%s\n' \
    '0000-00-00 00:00:00.123' \
    '0000-01-00 12:34:56.789' \
    '1970-01-01 00:00:00.123' \
    > "$DATA_FILE"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_05054"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_05054 (t DateTime64(3, 'UTC')) ENGINE = Memory"

for format in basic best_effort; do
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_05054"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_05054 FROM INFILE '${DATA_FILE}' SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0, date_time_input_format = '${format}' FORMAT TSV"
    $CLICKHOUSE_CLIENT -q "SELECT t FROM t_05054"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_05054"
