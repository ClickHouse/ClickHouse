#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without any grant on the table, `mergeTreeTextIndex` must not reveal whether the table
# is a MergeTree table or which indexes it has: every error must be ACCESS_DENIED.

user_name="${CLICKHOUSE_DATABASE}_user_05153"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_name;

CREATE TABLE tab
(
    a String,
    b String,
    INDEX idx_text a TYPE text(tokenizer = 'splitByNonAlpha'),
    INDEX idx_set b TYPE set(0)
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE tab_memory (a String) ENGINE = Memory;

INSERT INTO tab VALUES ('hello', 'world');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
"

function run_as_user()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1); then
        echo "OK"
    else
        echo "$output" | grep -oE '\([A-Z_]+\)' | tail -1 | tr -d '()'
    fi
}

run_as_user "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_text)"
run_as_user "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_set)"
run_as_user "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_missing)"
run_as_user "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab_memory, idx_text)"
run_as_user "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab_missing, idx_text)"

# A grant on any column implies SHOW TABLES, so index metadata becomes visible,
# while reading the index still requires SELECT on the indexed column.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(b) ON $CLICKHOUSE_DATABASE.tab TO $user_name"

run_as_user "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_text)"
run_as_user "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_set)"
run_as_user "SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab, idx_missing)"

$CLICKHOUSE_CLIENT -q "
DROP TABLE tab;
DROP TABLE tab_memory;
DROP USER $user_name;
"
