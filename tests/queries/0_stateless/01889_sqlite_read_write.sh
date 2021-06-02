#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_sqlite/test.sqlite3


${CLICKHOUSE_CLIENT} --query='DROP DATABASE IF EXISTS sqlite_database'
${CLICKHOUSE_CLIENT} --query="CREATE DATABASE sqlite_database ENGINE = SQLite('${DATA_FILE}')"

${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.`Some table`'

${CLICKHOUSE_CLIENT} --query='SELECT `some field` FROM sqlite_database.`Some table`;'

${CLICKHOUSE_CLIENT} --query='SELECT `string field` FROM sqlite_database.`Some table`;'

${CLICKHOUSE_CLIENT} --query='SELECT * FROM sqlite_database.`Empty table`;'

${CLICKHOUSE_CLIENT} --query='SELECT field2 FROM sqlite_database.`Empty table`;'

${CLICKHOUSE_CLIENT} --query='DROP DATABASE IF EXISTS sqlite_database;'
