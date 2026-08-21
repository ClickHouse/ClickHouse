#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading one file across several threads must not change the answers, must respect the order
# when asked to, and must survive a query that walks away early.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

# Enough rows for many splits - a split holds at most 100k - so several threads have work.
$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, toString(number) AS s, number % 100 AS k
    FROM numbers(1000000)
    FORMAT Vortex" > "$DATA_FILE"

echo "Aggregates do not depend on the number of threads or on the order:"
for threads in 1 4 16; do
    for preserve_order in 0 1; do
        $CLICKHOUSE_LOCAL -q "
            SELECT count(), sum(n), min(n), max(n), sum(length(s)), uniqExact(k)
            FROM file('$DATA_FILE', 'Vortex')
            SETTINGS max_parsing_threads = $threads, input_format_vortex_preserve_order = $preserve_order"
    done
done

echo "Rows come in file order with input_format_vortex_preserve_order:"
$CLICKHOUSE_LOCAL -q "
    SELECT count()
    FROM (SELECT n, rowNumberInAllBlocks() AS r FROM file('$DATA_FILE', 'Vortex'))
    WHERE n != r
    SETTINGS max_parsing_threads = 8, input_format_vortex_preserve_order = 1, max_threads = 1"

echo "Filter pushdown with several threads:"
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n >= 999990
    SETTINGS max_parsing_threads = 8, input_format_vortex_filter_push_down = 1"
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n >= 999990
    SETTINGS max_parsing_threads = 8, input_format_vortex_filter_push_down = 0"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), sum(n) FROM file('$DATA_FILE', 'Vortex') WHERE k = 7 AND s != '7'
    SETTINGS max_parsing_threads = 8"

echo "Stopping early does not hang or leave a driver behind:"
$CLICKHOUSE_LOCAL -q "SELECT n FROM file('$DATA_FILE', 'Vortex') LIMIT 3 FORMAT Null SETTINGS max_parsing_threads = 8"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM (SELECT n FROM file('$DATA_FILE', 'Vortex') LIMIT 5) SETTINGS max_parsing_threads = 8"

echo "A file with a single split needs no extra drivers:"
$CLICKHOUSE_LOCAL -q "SELECT number AS n FROM numbers(10) FORMAT Vortex" > "$DATA_FILE".small
$CLICKHOUSE_LOCAL -q "SELECT sum(n) FROM file('$DATA_FILE.small', 'Vortex') SETTINGS max_parsing_threads = 8"

rm -f "$DATA_FILE" "$DATA_FILE".small
