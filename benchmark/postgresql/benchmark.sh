#!/bin/bash

grep -v -P '^#' queries.sql | sed -e 's/{table}/hits_100m_pg/' | while read query; do

    echo 3 | sudo tee /proc/sys/vm/drop_caches

    echo "$query";
    for i in {1..3}; do
        # For some reason JIT does not work on my machine
        sudo -u postgres psql tutorial -t -c 'set jit = off' -c '\timing' -c "$query" | grep 'Time' | tee --append log
    done;
done;
