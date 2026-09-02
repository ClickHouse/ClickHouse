#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS data2;
DROP VIEW IF EXISTS mv1;

CREATE TABLE data (key Int32) ENGINE = Null;
CREATE TABLE data2 (key Int32) ENGINE = Null;
CREATE MATERIALIZED VIEW mv1 TO data2 AS SELECT * FROM data;
"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<< "INSERT INTO FUNCTION remote('127.{1,2}', $CLICKHOUSE_DATABASE, data, rand()) SELECT * FROM numbers(1e5)"
