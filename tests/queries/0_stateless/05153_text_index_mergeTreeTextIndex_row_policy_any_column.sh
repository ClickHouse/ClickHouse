#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A row policy on a column that is not indexed must still deny reading the text index:
# the dictionary contains the tokens of the rows the policy hides.

user_name="${CLICKHOUSE_DATABASE}_user_05153"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user_name;

CREATE TABLE tab
(
    tenant_id String,
    doc_id UInt64,
    secret_text String,
    INDEX idx_secret secret_text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY doc_id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES ('tenant_a', 1, 'visible apple phrase'), ('tenant_b', 2, 'hidden zebra phrase');

CREATE USER $user_name IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT ON $CLICKHOUSE_DATABASE.tab TO $user_name;
CREATE ROW POLICY p_05153 ON $CLICKHOUSE_DATABASE.tab FOR SELECT USING tenant_id = 'tenant_a' TO $user_name;
"

function run_as_user()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --user "$user_name" --password "password" -q "$1" 2>&1); then
        echo "$output"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo "ACCESS_DENIED"
    else
        echo "$output"
    fi
}

run_as_user "SELECT groupArray(secret_text) FROM tab"
run_as_user "SELECT count() FROM tab WHERE hasToken(secret_text, 'zebra')"
run_as_user "SELECT arraySort(groupArray(token)) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_secret)"

$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p_05153 ON $CLICKHOUSE_DATABASE.tab"

run_as_user "SELECT arraySort(groupArray(token)) FROM mergeTreeTextIndex(currentDatabase(), tab, idx_secret)"

$CLICKHOUSE_CLIENT -q "
DROP TABLE tab;
DROP USER $user_name;
"
