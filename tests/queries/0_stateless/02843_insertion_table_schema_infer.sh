#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_tsv

$CLICKHOUSE_LOCAL \
"CREATE VIEW users AS SELECT * FROM file('$DATA_DIR/mock_data.tsv', TSVWithNamesAndTypes); 
 CREATE TABLE users_output (name String, tag UInt64)ENGINE = Memory;
 INSERT INTO users_output WITH (SELECT groupUniqArrayArray(mapKeys(Tags)) FROM users) AS unique_tags SELECT UserName AS name, length(unique_tags) AS tag FROM users;
 SELECT * FROM users_output;"
