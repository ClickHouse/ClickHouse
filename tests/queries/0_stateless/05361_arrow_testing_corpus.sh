#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Arrow, Parquet and Avro support, and the fast test job does not check out `contrib/arrow-testing`.

# Read every data file of the Arrow test corpus https://github.com/apache/arrow-testing
# (the `contrib/arrow-testing` submodule) in the format it is written in: Arrow IPC files and streams, Parquet, Avro and CSV,
# including the fuzzer-generated files. For every file we print the format, the inferred schema, the number of rows
# and an order-independent hash of all values, or the name of the error if the file cannot be read.
# The files that ClickHouse has no format for (Arrow JSON integration files, Flight certificates, Protobuf schema) are skipped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CORPUS_DIR=$(realpath "$CUR_DIR/../../../contrib/arrow-testing")

if [ ! -f "$CORPUS_DIR/README.md" ]
then
    echo "The Arrow test corpus is not found in $CORPUS_DIR, run 'git submodule update --init contrib/arrow-testing'" >&2
    exit 1
fi

RESULTS_DIR="$CLICKHOUSE_TMP/arrow_testing_corpus_$CLICKHOUSE_DATABASE"
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

# Print the format of the file $1 or nothing if the file is not read.
function file_format()
{
    case "$1" in
        *.md | *.json.gz | *.json | *.proto | data/flight/*) ;;
        *.arrow_file | *.arrow | data/arrow-ipc-file/*) echo Arrow ;;
        *.stream | data/arrow-ipc-stream/* | data/arrow-ipc-tensor-stream/*) echo ArrowStream ;;
        data/parquet/timestamp.data) echo TSV ;;
        data/parquet/*) echo Parquet ;;
        data/avro/*) echo Avro ;;
        data/csv/fuzzing/*) echo CSV ;;
        data/csv/*) echo CSVWithNames ;;
        *) echo "Unknown file $1" >&2; exit 1 ;;
    esac
}

# Print the name of the error from the exception message in the file $1.
function error_name()
{
    # `-a`: the message may contain binary column names from the fuzzer-generated files.
    grep -a -m1 -oE '\(([A-Z][A-Z0-9_]+)\)$' "$1" | tr -d '()'
}

function read_file()
{
    local file=$1
    local format=$2
    local result
    result="$RESULTS_DIR/$(echo "$file" | tr '/' '_')"
    # Single-threaded reading, so that the error is deterministic for files with several corrupted columns.
    # Fixed time zone, because timestamps without a time zone are formatted in the session time zone.
    $CLICKHOUSE_LOCAL --max_threads=1 --max_parsing_threads=1 --session_timezone=UTC --print_pretty_type_names=0 --query "
        DESCRIBE file('$CORPUS_DIR/$file', $format) SETTINGS describe_compact_output = 1,
            format_custom_escaping_rule = 'Escaped', format_custom_field_delimiter = ' ', format_custom_row_between_delimiter = ', ',
            format_custom_row_after_delimiter = '', format_custom_result_after_delimiter = '\n'
            FORMAT CustomSeparated;
        SELECT count(), sum(sipHash64(toString(tuple(*)))) FROM file('$CORPUS_DIR/$file', $format);
    " > "$result.out" 2> "$result.err"

    {
        echo -ne "$file\t$format"
        [ -s "$result.out" ] && echo -ne "\t$(tr '\n' '\t' < "$result.out" | sed 's/\t$//')"
        [ -s "$result.err" ] && echo -ne "\tError: $(error_name "$result.err")"
        echo
    } > "$result"
}

export CORPUS_DIR RESULTS_DIR
export -f error_name read_file

FILES=""
for file in $(cd "$CORPUS_DIR" && find data -type f | LC_ALL=C sort)
do
    format=$(file_format "$file") || exit 1
    [ -n "$format" ] && FILES+="$file $format"$'\n'
done

echo -n "$FILES" | xargs -P 8 -L1 bash -c 'read_file "$@"' _

echo -n "$FILES" | while read -r file _
do
    cat "$RESULTS_DIR/$(echo "$file" | tr '/' '_')"
done

echo
echo "Integration files with different results for the Arrow IPC file and stream formats:"

for file in $(cd "$CORPUS_DIR" && find data/arrow-ipc-stream/integration -name '*.arrow_file' | LC_ALL=C sort)
do
    file_result="$RESULTS_DIR/$(echo "$file" | tr '/' '_')"
    stream_result="$RESULTS_DIR/$(echo "${file%.arrow_file}.stream" | tr '/' '_')"
    [ "$(cut -f3- "$file_result")" = "$(cut -f3- "$stream_result")" ] || echo "${file%.arrow_file}"
done

echo
echo "Comparison with the expected contents:"

file=data/parquet/generated_simple_numerics/blogs.parquet
expected=data/parquet/generated_simple_numerics/blogs.json
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE actual ENGINE = Memory AS SELECT * FROM file('$CORPUS_DIR/$file', Parquet);
    CREATE TABLE expected AS actual;
    INSERT INTO expected SELECT * FROM file('$CORPUS_DIR/$expected', JSONEachRow);
    SELECT '$file',
        (SELECT count() FROM actual),
        (SELECT count() FROM expected),
        (SELECT count() FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected)),
        (SELECT count() FROM (SELECT * FROM expected EXCEPT SELECT * FROM actual));
"

rm -rf "$RESULTS_DIR"
