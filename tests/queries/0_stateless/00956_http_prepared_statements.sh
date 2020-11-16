#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "DROP TABLE IF EXISTS ps";
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "CREATE TABLE ps (i UInt8, s String, d Date) ENGINE = Memory";

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "INSERT INTO ps VALUES (1, 'Hello, world', '2005-05-05')";
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "INSERT INTO ps VALUES (2, 'test', '2019-05-25')";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_id=1" \
    -d "SELECT * FROM ps WHERE i = {id:UInt8} ORDER BY i, s, d";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_phrase=Hello,+world" \
    -d "SELECT * FROM ps WHERE s = {phrase:String} ORDER BY i, s, d";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_date=2019-05-25" \
    -d "SELECT * FROM ps WHERE d = {date:Date} ORDER BY i, s, d";
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_id=2&param_phrase=test" \
    -d "SELECT * FROM ps WHERE i = {id:UInt8} and s = {phrase:String} ORDER BY i, s, d";

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "DROP TABLE ps";
