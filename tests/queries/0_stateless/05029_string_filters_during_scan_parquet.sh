#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not supported in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test for the `apply_string_filters_during_scan` setting with the Parquet format: substring search
# conditions from PREWHERE are pushed down into the decoding of string columns, and values that
# do not match them are decoded as empty strings. The result of every query must be the same
# with the setting enabled and disabled.

FILE_PLAIN="${CLICKHOUSE_DATABASE}/t_string_filter_plain.parquet"
FILE_DICT="${CLICKHOUSE_DATABASE}/t_string_filter_dict.parquet"

# A file with plain-encoded string pages (dictionary encoding is disabled)
# and a file with dictionary-encoded string pages (low-cardinality values).
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$FILE_PLAIN', Parquet, 'id UInt32, s String, n Nullable(String)')
SELECT
    number,
    multiIf(
        number % 11 = 0, '',
        number % 7 = 0, 'lorem ipsum needle dolor ' || toString(number),
        number % 5 = 0, 'needle at the start ' || toString(number),
        number % 3 = 0, toString(number) || ' ends with needle',
        'nothing interesting ' || toString(number)),
    multiIf(
        number % 13 = 0, NULL,
        number % 7 = 0, 'nullable needle ' || toString(number),
        'plain value ' || toString(number))
FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_max_dictionary_size = 0;

INSERT INTO FUNCTION file('$FILE_DICT', Parquet, 'id UInt32, s String, n Nullable(String)')
SELECT
    number,
    multiIf(
        number % 11 = 0, '',
        number % 7 = 0, 'lorem ipsum needle dolor',
        number % 5 = 0, 'needle at the start',
        number % 3 = 0, 'ends with needle',
        'nothing interesting ' || toString(number % 3)),
    multiIf(
        number % 13 = 0, NULL,
        number % 7 = 0, 'nullable needle',
        'plain value ' || toString(number % 5))
FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1;
"

for file in "$FILE_PLAIN" "$FILE_DICT"; do
    for enable in 0 1; do
        $CLICKHOUSE_CLIENT -q "
        SELECT count(), sum(cityHash64(s)), sum(cityHash64(id)) FROM file('$file', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(s)) FROM file('$file', Parquet) PREWHERE s LIKE 'needle%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(s)) FROM file('$file', Parquet) PREWHERE endsWith(s, 'needle') SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(s)) FROM file('$file', Parquet) PREWHERE position(s, 'needle') > 0 SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(s)), sum(cityHash64(n)) FROM file('$file', Parquet) PREWHERE s LIKE '%needle%' AND n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(n)) FROM file('$file', Parquet) PREWHERE n LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(s)) FROM file('$file', Parquet) PREWHERE s LIKE '%needle%' OR id = 1 SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(s)) FROM file('$file', Parquet) PREWHERE position(s, 'needle') = 0 SETTINGS apply_string_filters_during_scan = $enable;
        SELECT id, s, n FROM file('$file', Parquet) ORDER BY id LIMIT 3 SETTINGS apply_string_filters_during_scan = $enable;
        "
    done
done

echo 'the optimization is applied'
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM file('$FILE_PLAIN', Parquet) PREWHERE s LIKE '%rare-substring%' SETTINGS apply_string_filters_during_scan = 1, log_comment = '05029_string_filter_applied_plain'"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM file('$FILE_DICT', Parquet) PREWHERE s LIKE '%rare-substring%' SETTINGS apply_string_filters_during_scan = 1, log_comment = '05029_string_filter_applied_dict'"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT
    sum(ProfileEvents['StringValueFilterValuesChecked']) > 0,
    sum(ProfileEvents['StringValueFilterValuesReplaced']) > 0,
    sum(ProfileEvents['StringValueFilterBytesSkipped']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05029_string_filter_applied_plain';

SELECT
    sum(ProfileEvents['StringValueFilterValuesChecked']) > 0,
    sum(ProfileEvents['StringValueFilterValuesReplaced']) > 0,
    sum(ProfileEvents['StringValueFilterBytesSkipped']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05029_string_filter_applied_dict';
"

# Dictionary-encoded pages where the indexes come as RLE runs rather than as bit-packed indexes:
# a long run of a repeated index is materialized in bulk, and that path must honour the filter mask
# and report its selectivity exactly like the bit-packed one.
FILE_RLE="${CLICKHOUSE_DATABASE}/t_string_filter_rle.parquet"
# A single distinct value: the indexes are encoded with a zero bit width, i.e. one run for the page.
FILE_ONE_VALUE="${CLICKHOUSE_DATABASE}/t_string_filter_one_value.parquet"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$FILE_RLE', Parquet, 'id UInt32, s String')
SELECT number, if(intDiv(number, 10000) % 2 = 0, 'a long run without the substring', 'a long run with the needle inside')
FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('$FILE_ONE_VALUE', Parquet, 'id UInt32, s String')
SELECT number, 'the only value of the column'
FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1;
"

echo 'dictionary runs'
for file in "$FILE_RLE" "$FILE_ONE_VALUE"; do
    for enable in 0 1; do
        $CLICKHOUSE_CLIENT -q "
        SELECT count(), sum(cityHash64(s)) FROM file('$file', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT count(), sum(cityHash64(s)) FROM file('$file', Parquet) PREWHERE s LIKE '%value%' SETTINGS apply_string_filters_during_scan = $enable;
        SELECT id, s FROM file('$file', Parquet) ORDER BY id LIMIT 2 SETTINGS apply_string_filters_during_scan = $enable;
        "
    done
done

echo 'the optimization is applied to the runs'
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM file('$FILE_RLE', Parquet) PREWHERE s LIKE '%rare-substring%' SETTINGS apply_string_filters_during_scan = 1, log_comment = '05029_string_filter_applied_rle'"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM file('$FILE_ONE_VALUE', Parquet) PREWHERE s LIKE '%rare-substring%' SETTINGS apply_string_filters_during_scan = 1, log_comment = '05029_string_filter_applied_one_value'"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT
    log_comment,
    sum(ProfileEvents['StringValueFilterValuesChecked']) > 0,
    sum(ProfileEvents['StringValueFilterValuesReplaced']) > 0,
    sum(ProfileEvents['StringValueFilterBytesSkipped']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05029_string_filter_applied_rle', '05029_string_filter_applied_one_value')
GROUP BY log_comment ORDER BY log_comment;
"

# Row group pruning materializes the whole dictionary of the same column to build its value set.
# That pass sees every distinct value exactly once, which says nothing about how often the scan will
# meet it, so it must not feed the selectivity statistics of the shared filter: here almost every
# dictionary entry matches `%needle%`, while most of the rows do not, and the optimization must stay on.
FILE_PRUNE="${CLICKHOUSE_DATABASE}/t_string_filter_prune.parquet"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$FILE_PRUNE', Parquet, 'id UInt32, s String')
SELECT number, if(number < 70000, 'needle ' || toString(number), 'an ordinary value')
FROM numbers(200000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_max_dictionary_size = 16777216;
"

echo 'dictionary pruning does not disable the filter'
$CLICKHOUSE_CLIENT -q "
SELECT count() FROM file('$FILE_PRUNE', Parquet)
PREWHERE s IN ('needle 5', 'needle 7') AND s LIKE '%needle%'
SETTINGS apply_string_filters_during_scan = 1, input_format_parquet_dictionary_filter_push_down = 16777216,
    log_comment = '05029_string_filter_pruning'"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT sum(ProfileEvents['StringValueFilterValuesReplaced']) > 100000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05029_string_filter_pruning';
"
