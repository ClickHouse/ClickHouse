#!/bin/bash

model_formatter="/home/natasha/MurfelClickHouse/build/programs/clickhouse-format";
new_formatter="/home/natasha/copyClickHouse/build/programs/clickhouse-format";
compare="/home/natasha/copyClickHouse/tests/a.out"

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

        $compare """$expected""" """$actual""";
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
