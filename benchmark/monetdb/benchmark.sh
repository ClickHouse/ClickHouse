#!/bin/bash

grep -v -P '^#' queries.sql | sed -e 's/{table}/hits/' | while read query; do
    echo 3 | sudo tee /proc/sys/vm/drop_caches

    echo "$query";
    for i in {1..3}; do
        ./send-query "$query" 2>&1 | grep -P '\d+ tuple|clk: |unknown|overflow|error';
    done;
done;
