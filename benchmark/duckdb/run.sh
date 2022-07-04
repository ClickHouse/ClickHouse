#!/bin/bash

cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    # We need to restart and reinitialize DuckDB after every query,
    # because it often fails with Segmentation fault (core dumped)
    ./query.py <<< "${query}"
done
