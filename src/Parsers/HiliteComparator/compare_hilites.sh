#!/bin/bash

# This script asserts that two versions of clickhouse-format format queries from 'tests/queries/0_stateless' in the same way.
# All queries that are formatted differently will be printed to stdout.
#
# This script uses hilite_comparator in order to compare hilited queries, which cannot be compared byte-by-byte.
#
# An example use-case is to assert that a refactoring didn't affect query formatting.
#
# Build two versions of clickhouse-format and hilite_comparator before using the script:
# cd build && ninja clickhouse-format format_comparator

model_formatter=$1  # e.g. "ClickHouse/build/programs/clickhouse-format";
new_formatter=$2  # e.g. "refactorClickHouse/build/programs/clickhouse-format";
hilite_comparator=$3  # e.g. "refactorClickHouse/build/src/Parsers/HiliteComparator/hilite_comparator"

# todo: uncomment oneline to test correct newlines as well
flags="--oneline --hilite --query";

for queries_file in /home/natasha/copyClickHouse/tests/queries/0_stateless/*.sql
do
    #echo "$queries_file";
    queries_text=$(cat "$queries_file");
    IFS=';'; queries=($queries_text); unset IFS;
    for query in "${queries[@]}"
    do
        expected=$($model_formatter $flags """$query""" 2>/dev/null);
        if [ $? -ne 0 ]; then
            #echo "Skipping"  # Mostly insert queries which are not supported by clickhouse-format
            #echo "Query: " $query
            continue;
        fi
        actual=$($new_formatter $flags """$query""" 2>/dev/null);
        if [ $? -ne 0 ]; then
            echo "[ FAIL ] New clickhouse-format exited with non-null status";
        fi

        $hilite_comparator """$expected""" """$actual""";
        if [ $? -ne 0 ]; then
            echo "[ FAIL ] Hilited queries do not match";
            echo "File: " "$queries_file";
            echo "Expected:"
            echo "$expected";
            echo "Actual:"
            echo "$actual";
        fi
    done
done
