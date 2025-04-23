#!/usr/bin/env bash
BIN="../../../build/programs/clickhouse client"

QUERY="SELECT * FROM s3('http://localhost:9000/test/hello.csv', 'secret', 'secret', 'CSV', 'value String');"

${BIN} --query "${QUERY}" &
pid=$!

sleep 1

ps -p "$pid" -o args=


QUERY="SELECT * FROM s3('http://localhost:9000/test/hello.csv', 'secret', 'secret', 'CSV', 'value String');"

${BIN} --query="${QUERY}" &
pid=$!

sleep 1

ps -p "$pid" -o args=
