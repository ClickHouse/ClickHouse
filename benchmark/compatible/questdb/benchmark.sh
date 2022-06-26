#!/bin/bash

# Install

wget https://github.com/questdb/questdb/releases/download/6.4.1/questdb-6.4.1-rt-linux-amd64.tar.gz
tar xf questdb*.tar.gz
questdb-6.4.1-rt-linux-amd64/bin/questdb.sh start

# Import the data

curl -G --data-urlencode "query=$(cat create.sql)" 'http://localhost:9000/exec?timings=true'
time curl -F data=@hits.csv 'http://localhost:9000/imp?name=hits'
