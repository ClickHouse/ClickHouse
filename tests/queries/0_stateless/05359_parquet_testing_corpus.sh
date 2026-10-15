#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support, and the fast test job does not check out `contrib/parquet-testing`.

# Read every file of the Parquet test corpus https://github.com/apache/parquet-testing
# (the `contrib/parquet-testing` submodule).
# For every file we print the inferred schema, the number of rows and an order-independent hash of all values,
# or the name of the error if the file cannot be read. Files with expected contents in CSV are compared with them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CORPUS_DIR=$(realpath "$CUR_DIR/../../../contrib/parquet-testing")

if [ ! -f "$CORPUS_DIR/README.md" ]
then
    echo "The Parquet test corpus is not found in $CORPUS_DIR, run 'git submodule update --init contrib/parquet-testing'" >&2
    exit 1
fi

RESULTS_DIR="$CLICKHOUSE_TMP/parquet_testing_corpus_$CLICKHOUSE_DATABASE"
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

# Print the name of the error from the exception message in the file $1.
function error_name()
{
    grep -m1 -oE '\(([A-Z][A-Z0-9_]+)\)$' "$1" | tr -d '()'
}

function read_file()
{
    local file=$1
    local result
    result="$RESULTS_DIR/$(echo "$file" | tr '/' '_')"
    # Single-threaded reading, so that the error is deterministic for files with several corrupted columns.
    # Fixed time zone, because timestamps without a time zone inside variants are formatted in the session time zone.
    # Fixed memory limit, because some files decompress to gigabytes (a 1 GiB string in `large_string_map.brotli.parquet`),
    # and whether they can be read would otherwise depend on the available memory.
    $CLICKHOUSE_LOCAL --max_threads=1 --max_parsing_threads=1 --session_timezone=UTC --print_pretty_type_names=0 --max_memory_usage=1G --query "
        DESCRIBE file('$CORPUS_DIR/$file', Parquet) SETTINGS describe_compact_output = 1,
            format_custom_escaping_rule = 'Raw', format_custom_field_delimiter = ' ', format_custom_row_between_delimiter = ', ',
            format_custom_row_after_delimiter = '', format_custom_result_after_delimiter = '\n'
            FORMAT CustomSeparated;
        SELECT count(), sum(sipHash64(toString(tuple(*)))) FROM file('$CORPUS_DIR/$file', Parquet);
    " > "$result.out" 2> "$result.err"

    {
        echo -ne "$file"
        [ -s "$result.out" ] && echo -ne "\t$(tr '\n' '\t' < "$result.out" | sed 's/\t$//')"
        [ -s "$result.err" ] && echo -ne "\tError: $(error_name "$result.err")"
        echo
    } > "$result"
}

export CORPUS_DIR RESULTS_DIR
export -f error_name read_file

FILES=$(cd "$CORPUS_DIR" && find . -type f \( -name '*.parquet' -o -name '*.parquet.encrypted' \) -not -path './.git/*' | sed "s|^\./||" | LC_ALL=C sort)

echo "$FILES" | xargs -P 8 -I{} bash -c 'read_file "$@"' _ {}

for file in $FILES
do
    cat "$RESULTS_DIR/$(echo "$file" | tr '/' '_')"
done

echo
echo "Comparison with the expected contents:"

for expected in $(cd "$CORPUS_DIR" && ls data/*_expect.csv)
do
    file="${expected%_expect.csv}.parquet"
    # The header of the CSV files may differ from the column names, so it is ignored.
    $CLICKHOUSE_LOCAL --input_format_with_names_use_header=0 --query "
        CREATE TABLE actual ENGINE = Memory AS SELECT * FROM file('$CORPUS_DIR/$file', Parquet);
        CREATE TABLE expected AS actual;
        INSERT INTO expected SELECT * FROM file('$CORPUS_DIR/$expected', CSVWithNames);
        SELECT '$file',
            (SELECT count() FROM actual),
            (SELECT count() FROM expected),
            (SELECT count() FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected)),
            (SELECT count() FROM (SELECT * FROM expected EXCEPT SELECT * FROM actual));
    "
done

rm -rf "$RESULTS_DIR"
