#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_null.data

# Wrapper for clickhouse-client to always output in JSONEachRow format, that
# way format settings will not affect output.
function clickhouse_local()
{
    $CLICKHOUSE_LOCAL --output-format JSONEachRow "$@"
}

echo "TSV"

echo 'Custom NULL representation' > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)') SETTINGS format_tsv_null_representation='Custom NULL representation'"

echo -e 'N\tU\tL\tL' > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)') SETTINGS format_tsv_null_representation='N\tU\tL\tL'"

echo -e "\\NSome text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)')"

echo -e "\\N" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)')"

echo -e "\\NSome text\n\\N\nSome more text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)')"

echo -e "\\N\n\\N\nSome more text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)')"

echo -e "1\t\\NSome text\t1" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 'x Int32, s Nullable(String), y Int32')"

echo -e "1\t\\N\t1" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 'x Int32, s Nullable(String), y Int32')"

echo -e "CustomNullSome text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)') SETTINGS format_tsv_null_representation='CustomNull'"

echo -e "CustomNullSome text\nCustomNull\nSome more text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)') SETTINGS format_tsv_null_representation='CustomNull'"

echo -e "CustomNull\nCustomNull\nSome more text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)') SETTINGS format_tsv_null_representation='CustomNull'"

echo -e "1\tCustomNull\t1" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 'x Int32, s Nullable(String), y Int32') SETTINGS format_tsv_null_representation='CustomNull'"

echo -e "1\tCustomNull\t1" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 'x Int32, s Nullable(String), y Int32') SETTINGS format_tsv_null_representation='CustomNull'"


echo "CSV"

echo 'Custom NULL representation' > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)') SETTINGS format_csv_null_representation='Custom NULL representation'"

echo -e 'N,U,L,L' > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)') SETTINGS format_csv_null_representation='N,U,L,L'"

echo -e "\\NSome text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)')"

echo -e "\\N" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)')"

echo -e "\\NSome text\n\\N\nSome more text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)')"

echo -e "\\N\n\\N\nSome more text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)')"

echo -e "1,\\NSome text,1" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 'x Int32, s Nullable(String), y Int32')"

echo -e "1,\\N,1" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 'x Int32, s Nullable(String), y Int32')"

echo -e "CustomNullSome text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)') SETTINGS format_csv_null_representation='CustomNull'"

echo -e "CustomNullSome text\nCustomNull\nSome more text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)') SETTINGS format_csv_null_representation='CustomNull'"

echo -e "CustomNull\nCustomNull\nSome more text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's Nullable(String)') SETTINGS format_csv_null_representation='CustomNull'"

echo -e "1,CustomNull,1" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 'x Int32, s Nullable(String), y Int32') SETTINGS format_csv_null_representation='CustomNull'"

echo -e "1,CustomNull,1" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 'x Int32, s Nullable(String), y Int32') SETTINGS format_csv_null_representation='CustomNull'"


echo 'Corner cases'
echo 'TSV'

echo -e "Some text\tCustomNull" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's String, n Nullable(String)') settings storage_file_read_method='pread', max_read_buffer_size=15, format_tsv_null_representation='CustomNull', input_format_parallel_parsing=0, input_format_tsv_detect_header=0"

echo -e "Some text\tCustomNull Some text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's String, n Nullable(String)') settings storage_file_read_method='pread', max_read_buffer_size=15, format_tsv_null_representation='CustomNull', input_format_parallel_parsing=0, input_format_tsv_detect_header=0"

echo -e "Some text\t123NNN" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's String, n Nullable(Int32)') settings storage_file_read_method='pread', max_read_buffer_size=14, format_tsv_null_representation='123NN', input_format_parallel_parsing=0, input_format_tsv_detect_header=0" 2>&1 | grep -F -q "CANNOT_READ_ALL_DATA" && echo 'OK' || echo 'FAIL'

echo -e "Some text\tNU\tLL" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's String, n Nullable(String)') settings storage_file_read_method='pread', max_read_buffer_size=13, format_tsv_null_representation='NU\tL', input_format_parallel_parsing=0, input_format_tsv_detect_header=0" 2>&1 | grep -F -q "CANNOT_READ_ALL_DATA" && echo 'OK' || echo 'FAIL'

echo 'CSV'

echo -e "Some text,CustomNull" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's String, n Nullable(String)') settings storage_file_read_method='pread', max_read_buffer_size=15, format_csv_null_representation='CustomNull', input_format_parallel_parsing=0, input_format_csv_detect_header=0"

echo -e "Some text,CustomNull Some text" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's String, n Nullable(String)') settings storage_file_read_method='pread', max_read_buffer_size=15, format_csv_null_representation='CustomNull', input_format_parallel_parsing=0, input_format_csv_detect_header=0"

echo -e "Some text,123NNN\n" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's String, n Nullable(Int32)') settings storage_file_read_method='pread', max_read_buffer_size=14, format_csv_null_representation='123NN', input_format_parallel_parsing=0, input_format_csv_detect_header=0" 2>&1 | grep -F -q "CANNOT_READ_ALL_DATA" && echo 'OK' || echo 'FAIL'

echo -e "Some text,NU,LL\n" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'CSV', 's String, n Nullable(String)') settings storage_file_read_method='pread', max_read_buffer_size=13, format_csv_null_representation='NU,L', input_format_parallel_parsing=0, input_format_csv_detect_header=0" 2>&1 | grep -F -q "CANNOT_READ_ALL_DATA" && echo 'OK' || echo 'FAIL'


echo 'Large custom NULL'

$CLICKHOUSE_LOCAL -q "select '0000000000Custom NULL representation0000000000' FROM numbers(10)" > $DATA_FILE
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)') SETTINGS storage_file_read_method='pread', max_read_buffer_size=5, input_format_parallel_parsing=0, format_tsv_null_representation='0000000000Custom NULL representation0000000000', input_format_tsv_detect_header=0"
clickhouse_local -q "SELECT * FROM file('$DATA_FILE', 'TSV', 's Nullable(String)') SETTINGS storage_file_read_method='pread', max_read_buffer_size=5, input_format_parallel_parsing=0, format_tsv_null_representation='0000000000Custom NULL representation000000000', input_format_tsv_detect_header=0"

rm $DATA_FILE
