#!/usr/bin/env bash
# Tags: no-fasttest
# The interserver secret needs the SSL library, which the fast test build does not have.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Through an interserver connection the shard authenticates the initiating user, so the checks of
# `mergeTreeTextIndex` apply to that user instead of refusing the query as over an ordinary connection.

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

query="SELECT arraySort(groupUniqArray(token)) FROM cluster('test_cluster_interserver_secret', mergeTreeTextIndex('$CLICKHOUSE_DATABASE', 'tab', 'idx_s')) SETTINGS prefer_localhost_replica = 0"

run_as_user "$query"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $CLICKHOUSE_DATABASE.tab TO $user_name"

run_as_user "$query"

$CLICKHOUSE_CLIENT -q "
DROP TABLE tab;
DROP USER $user_name;
"
