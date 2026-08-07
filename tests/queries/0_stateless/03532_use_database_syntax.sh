#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

database_name="$CLICKHOUSE_DATABASE"_d1

${CLICKHOUSE_CLIENT} "
CREATE DATABASE IF NOT EXISTS $database_name;

CREATE TABLE IF NOT EXISTS $database_name.t1 (val Int) engine=Memory;
INSERT INTO $database_name.t1 SELECT 1;
"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM t1" 2>&1 | grep -q 'UNKNOWN_TABLE' || echo 'Missing UNKNOWN_TABLE error'

${CLICKHOUSE_CLIENT} "
USE DATABASE $database_name;
SELECT * FROM t1;
"

${CLICKHOUSE_CLIENT} "
DROP TABLE $database_name.t1;
DROP DATABASE $database_name;
"

database_name="$CLICKHOUSE_DATABASE"_database

${CLICKHOUSE_CLIENT} --query="CREATE DATABASE IF NOT EXISTS $database_name"

${CLICKHOUSE_CLIENT} --query="USE DATABASE $database_name"
${CLICKHOUSE_CLIENT} --query="USE $database_name"

${CLICKHOUSE_CLIENT} --query="DROP DATABASE $database_name"
