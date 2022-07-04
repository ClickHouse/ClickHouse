#!/bin/bash

TRIES=3

cat queries.sql | while read query; do
    for i in $(seq 1 $TRIES); do
        aws athena --output json start-query-execution --query-execution-context 'Database=test' --result-configuration "OutputLocation=${OUTPUT}" --query-string "${query}" | jq '.QueryExecutionId'
    done
done
