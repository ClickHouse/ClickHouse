#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    for i in $(seq 1 $TRIES); do
        mysql -h "${HOST}" -u admin --password="${PASSWORD}" test -vvv -e "${query}"
    done;
done;
