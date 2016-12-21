#!/usr/bin/env bash

clickhouse-client --host=localhost --query="SELECT * FROM ext" --format=Vertical --external --file=- --structure="s String" --name=ext --format=JSONEachRow <<< '{"s":"Hello"}'
