#!/usr/bin/env bash

for ((p = 2; p <= 10; p++))
do
    for ((i = 1; i <= 9; i++))
    do
        n=$(( 10**p * i ))
        echo -n "$n "
        clickhouse-client -q "select uniqHLL12(number), uniq(number), uniqCombined(number) from numbers($n);"
    done
done
