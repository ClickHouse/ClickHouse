#!/bin/bash

grep -v -P '^#' queries.sql | sed -e 's/{table}/hits/' | while read query; do

    echo 3 | sudo tee /proc/sys/vm/drop_caches
    sudo systemctl restart omnisci_server
    for i in {1..1000}; do
        /opt/omnisci/bin/omnisql -t -p HyperInteractive <<< "SELECT 1;" 2>&1 | grep -q '1 rows returned' && break;
        sleep 0.1;
    done
    sleep 10;

    echo "$query";
    for i in {1..3}; do
        /opt/omnisci/bin/omnisql -t -p HyperInteractive <<< "$query" 2>&1 | grep -P 'Exception:|Execution time:';
    done;
done;
