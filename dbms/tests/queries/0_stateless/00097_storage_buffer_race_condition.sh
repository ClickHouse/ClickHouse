#!/bin/sh
seq 1 1000 | sed -r 's/.+/CREATE TABLE IF NOT EXISTS test.buf (a UInt8) ENGINE = Buffer(test, b, 1, 1, 1, 1, 1, 1, 1); DROP TABLE test.buf;/' | clickhouse-client -n
