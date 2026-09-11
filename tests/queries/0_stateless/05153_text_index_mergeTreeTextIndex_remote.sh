#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `remote` over a local shard validates the access of the current user while inferring the structure of a nested
# table function, and may then route the query over loopback with other credentials. `mergeTreeTextIndex` has a
# static structure, so it has to check the access of the user itself there.

user_name="${CLICKHOUSE_DATABASE}_user_05153"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_name;

CREATE TABLE tab (s String, INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('hidden token');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT REMOTE ON *.* TO $user_name;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_name;
"

function run_as_user()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1); then
        echo "$output"
    else
        echo "$output" | grep -oE '\([A-Z_]+\)' | tail -1 | tr -d '()'
    fi
}

index="mergeTreeTextIndex('$CLICKHOUSE_DATABASE', 'tab', 'idx_s')"
query="SELECT count() FROM remote('127.0.0.1:$CLICKHOUSE_PORT_TCP', $index)"

run_as_user "DESCRIBE TABLE $index"
run_as_user "$query SETTINGS prefer_localhost_replica = 0"
run_as_user "$query SETTINGS prefer_localhost_replica = 1"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.tab TO $user_name"

run_as_user "$query SETTINGS prefer_localhost_replica = 0"
run_as_user "$query SETTINGS prefer_localhost_replica = 1"

$CLICKHOUSE_CLIENT -q "
DROP TABLE tab;
DROP USER $user_name;
"
